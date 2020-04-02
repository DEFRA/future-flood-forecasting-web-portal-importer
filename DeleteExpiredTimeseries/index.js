const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const insertDataIntoTemp = require('./insertDataIntoTempTable')
const moment = require('moment')
const sql = require('mssql')

module.exports = async function (context, myTimer) {
  // current time
  const timeStamp = moment().format()

  if (myTimer.isPastDue) {
    context.log('JavaScript is running late!')
  }

  if (process.env['DELETE_EXPIRED_TIMESERIES_HARD_LIMIT']) {
    // The read-commited isolation level allows reads, writes and deletes on table data whilst the delete is
    // running (the locks are released after reading, there are no modified objects in the query so no further locks should take place).
    // Read commited ensures only commited data is selected to delete. Read commited does not protect against Non-repeatable reads or Phantom reads,
    // however the higher isolation levels (given the nature of the queries in the transaction) do not justify the concurrency cost in this case.
    await doInTransaction(removeExpiredTimeseries, context, 'The expired timeseries deletion has failed with the following error:', sql.ISOLATION_LEVEL.READ_COMMITTED)
  } else {
    context.log.warn('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT needs setting before timeseries can be removed.')
    throw new Error(`DELETE_EXPIRED_TIMESERIES_HARD_LIMIT needs setting before timeseries can be removed.`)
  }

  async function removeExpiredTimeseries (transaction, context) {
    // current date    :-------------------------------------->|
    // soft date       :---------------------|                  - delete all completed records before this date
    // hard date       :------------|                           - delete all records before this date
    let hardDate
    let softDate
    const hardLimit = parseInt(process.env['DELETE_EXPIRED_TIMESERIES_HARD_LIMIT'])
    const softLimit = process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT'] ? parseInt(process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT']) : hardLimit
    // Dates need to be specified as UTC using ISO 8601 date formatting manually to ensure portability between local and cloud environments.
    // Not using toUTCString() as toISOString() supports ms.
    if (hardLimit > 0 && hardLimit !== undefined && !isNaN(hardLimit)) {
      // This check is required to prevent zero subtraction, the downstream effect would be the removal of all data prior to the current date.
      hardDate = moment.utc().subtract(hardLimit, 'hours').toDate().toISOString()
      if (softLimit <= hardLimit && !isNaN(softLimit)) { // if the soft limit is undefined it defaults to the hard limit.
        softDate = moment.utc().subtract(softLimit, 'hours').toDate().toISOString()
      } else {
        context.log.error(`The soft-limit must be an integer and less than or equal to the hard-limit.`)
        throw new Error('DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT must be an integer and less than or equal to the hard-limit.')
      }
    } else {
      context.log.error(`The hard-limit must be an integer greater than 0.`)
      throw new Error('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT must be an integer greater than 0.')
    }

    await createTempTable(transaction, context)

    await executePreparedStatementInTransaction(insertDataIntoTemp, context, transaction, hardDate, false)
    await executePreparedStatementInTransaction(insertDataIntoTemp, context, transaction, softDate, true)

    context.log.info(`Data delete starting.`)
    await executePreparedStatementInTransaction(deleteReportingRows, context, transaction)
    await executePreparedStatementInTransaction(deleteTimeseriesRows, context, transaction)
    await executePreparedStatementInTransaction(deleteHeaderRows, context, transaction)

    context.log('JavaScript timer trigger function ran!', timeStamp)
  }
  // context.done() is not requried as there is no output binding to be activated.
}

async function createTempTable (transaction, context) {
  context.log.info(`Building temp table`)
  // Create a local temporary table to store deletion jobs
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
