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
    // The default limits can be overridden by the two environment variables
    const hardLimit = process.env['HARD_EXPIRY_LIMIT'] ? parseInt(process.env['HARD_EXPIRY_LIMIT']) : 48
    const softLimit = process.env['SOFT_EXPIRY_LIMIT'] ? parseInt(process.env['SOFT_EXPIRY_LIMIT']) : 24

    // Limits are in hours
    // Dates need to be specified as UTC using ISO 8601 date formatting manually to ensure portability between local and cloud environments.
    // Any timeseries older than the hard date will be removed. Not using toUTCString() as toISOSTRInG() supports ms.
    const hardDate = moment.utc().subtract(hardLimit, 'hours').toDate().toISOString()
    const softDate = moment.utc().subtract(softLimit, 'hours').toDate().toISOString()

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
  await doInTransaction(removeExpiredTimeseries, context, 'The expired timeseries deletion has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as there is no output binding to be activated.
}

async function createTempTable (transaction, context) {
  // Create a local temporary table
  await new sql.Request(transaction).batch(`
      create table #deletion_job_temp
      (
        id uniqueidentifier not null default newid(),
        timeseries_id uniqueidentifier not null,
        timeseries_header_id uniqueidentifier not null,
        job_status int not null
      )
    `)
}

async function insertSoftDataIntoTemp (context, preparedStatement, softDate) {
  const FME_COMPLETE_JOB_STATUS = parseInt(process.env['FME_COMPLETE_JOB_STATUS'])

  await preparedStatement.input('softDate', sql.DateTime2)
  await preparedStatement.input('completeStatus', sql.Int)

  await preparedStatement.prepare(
    `insert into #deletion_job_temp (id, timeseries_id, job_status, timeseries_header_id)
    select r.id, r.timeseries_id, r.job_status, t.timeseries_header_id
    from [${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}].timeseries_job r
      join [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].timeseries t on t.id = r.timeseries_id
      join [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].timeseries_header h on t.timeseries_header_id = h.id
    where
      r.job_status = @completeStatus
      and
      h.import_time < cast(@softDate as datetime2)`
  )

  // console.log(preparedStatement.statement)

  const parameters = {
    softDate: softDate,
    completeStatus: FME_COMPLETE_JOB_STATUS
  }

  await preparedStatement.execute(parameters)
}

async function insertHardDataIntoTemp (context, preparedStatement, hardDate) {
  await preparedStatement.input('hardDate', sql.DateTime2)

  await preparedStatement.prepare(
    `insert into #deletion_job_temp (id, timeseries_id, job_status, timeseries_header_id)
    select r.id, r.timeseries_id, r.job_status, t.timeseries_header_id
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
    on te.id = r.ID`
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
