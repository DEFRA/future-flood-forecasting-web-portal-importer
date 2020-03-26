const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const moment = require('moment')
const sql = require('mssql')

module.exports = async function (context, myTimer) {
  // current time
  const timeStamp = moment().format()

  if (myTimer.isPastDue) {
    context.log('JavaScript is running late!')
  }
  async function removeExpiredTimeseries (transaction, context) {
    const hardLimit = parseInt(process.env['DELETE_EXPIRED_TIMESERIES_HARD_LIMIT'])
    // If no soft limit is specified it inherits the hard limit
    const softLimit = process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT'] ? parseInt(process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT']) : hardLimit

    // Limits are in hours
    // Dates need to be specified as UTC using ISO 8601 date formatting manually to ensure portability between local and cloud environments.
    // Any timeseries older than the hard date will be removed. Not using toUTCString() as toISOSTRInG() supports ms.
    const hardDate = moment.utc().subtract(hardLimit, 'hours').toDate().toISOString()
    const softDate = moment.utc().subtract(softLimit, 'hours').toDate().toISOString()
    // current date     :-------------------------------------->|
    // soft date        :---------------------|                  - delete all completed records before this date
    // hard date        :------------|                           - delete all records before this date

    await createTempTable(transaction, context)

    await executePreparedStatementInTransaction(insertHardDataIntoTemp, context, transaction, hardDate)
    await executePreparedStatementInTransaction(insertSoftDataIntoTemp, context, transaction, softDate)

    await executePreparedStatementInTransaction(deleteReportingRows, context, transaction)
    await executePreparedStatementInTransaction(deleteTimeseriesRows, context, transaction)
    await executePreparedStatementInTransaction(deleteHeaderRows, context, transaction)

    context.log('JavaScript timer trigger function ran!', timeStamp)
  }

  // Refresh with a READ COMMITTED isolation level (the SQL server default). Allows a transaction to read data previously read (not modified)
  // by another transaction without waiting for the first transaction to complete. The SQL Server Database Engine keeps write
  // locks (acquired on selected data) until the end of the transaction, but read locks are released as soon as the SELECT operation is performed.
  if (process.env['DELETE_EXPIRED_TIMESERIES_HARD_LIMIT']) {
    await doInTransaction(removeExpiredTimeseries, context, 'The expired timeseries deletion has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
    // context.done() not requried as there is no output binding to be activated.
  } else {
    context.log.warn('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT needs setting before timeseries can be removed.')
  }
}

async function createTempTable (transaction, context) {
  // Create a local temporary table
  await new sql.Request(transaction).batch(`
      create table #deletion_job_temp
      (
        reporting_id uniqueidentifier not null,
        timeseries_id uniqueidentifier not null,
        timeseries_header_id uniqueidentifier not null
      )
      CREATE CLUSTERED INDEX ix_deletion_job_temp_reporting_id
        ON #deletion_job_temp (reporting_id)
      CREATE INDEX ix_deletion_job_temp_timeseries_id
        ON #deletion_job_temp (timeseries_id)
      CREATE INDEX ix_deletion_job_temp_timeseries_header_id
        ON #deletion_job_temp (timeseries_header_id)
    `)
}

async function insertSoftDataIntoTemp (context, preparedStatement, softDate) {
  const FME_COMPLETE_JOB_STATUS = 6

  await preparedStatement.input('softDate', sql.DateTime2)
  await preparedStatement.input('completeStatus', sql.Int)

  await preparedStatement.prepare(
    `insert into #deletion_job_temp (reporting_id, timeseries_id, timeseries_header_id)
    select r.id, r.timeseries_id, t.timeseries_header_id
    from [${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}].timeseries_job r
      join [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].timeseries t on t.id = r.timeseries_id
      join [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].timeseries_header h on t.timeseries_header_id = h.id
    where
      r.job_status = @completeStatus
      and
      h.import_time < cast(@softDate as datetime2)`
  )

  const parameters = {
    softDate: softDate,
    completeStatus: FME_COMPLETE_JOB_STATUS
  }

  await preparedStatement.execute(parameters)
}

async function insertHardDataIntoTemp (context, preparedStatement, hardDate) {
  await preparedStatement.input('hardDate', sql.DateTime2)

  await preparedStatement.prepare(
    `insert into #deletion_job_temp (reporting_id, timeseries_id, timeseries_header_id)
    select r.id, r.timeseries_id, t.timeseries_header_id
    from [${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}].timeseries_job r
      join [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].timeseries t on t.id = r.timeseries_id
      join [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].timeseries_header h on t.timeseries_header_id = h.id
    where
      h.import_time < cast(@hardDate as datetime2)`
  )
  const parameters = {
    hardDate: hardDate
  }
  await preparedStatement.execute(parameters)
}

async function deleteReportingRows (context, preparedStatement) {
  await preparedStatement.prepare(
    `delete r from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB r
    inner join #deletion_job_temp te
    on te.reporting_id = r.id`
  )

  await preparedStatement.execute()
}

async function deleteTimeseriesRows (context, preparedStatement) {
  await preparedStatement.prepare(
    `delete t from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES t
    inner join #deletion_job_temp te
    on te.timeseries_id = t.id`
  )

  await preparedStatement.execute()
}

async function deleteHeaderRows (context, preparedStatement) {
  await preparedStatement.prepare(
    `delete th from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES_HEADER th
    inner join #deletion_job_temp te
    on te.timeseries_header_id = th.id`
  )

  await preparedStatement.execute()
}
