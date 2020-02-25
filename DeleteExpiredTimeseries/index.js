const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
// const createStagingException = require('../Shared/create-staging-exception')
const moment = require('moment')
const sql = require('mssql')

module.exports = async function (context, myTimer) {
  // current time
  var timeStamp = moment()

  if (myTimer.isPastDue) {
    context.log('JavaScript is running late!')
  }
  context.log('JavaScript timer trigger function ran!', timeStamp)

  async function removeExpiredTimeseries (transaction, context) {
    await createTempTable(transaction, context)

    await executePreparedStatementInTransaction(insertSoftDataIntoTemp, context, transaction)
    await executePreparedStatementInTransaction(insertHardDataIntoTemp, context, transaction)

    await executePreparedStatementInTransaction(deleteReportingRows, context, transaction)
    await executePreparedStatementInTransaction(deleteTimeseriesRows, context, transaction)
    await executePreparedStatementInTransaction(deleteHeaderRows, context, transaction)
  }

  // Refresh with a serializable isolation level so that record deletion is prevented if the tables are in use.
  // If the table is in use and table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, manual intervention will be required.
  await doInTransaction(removeExpiredTimeseries, context, 'The expired timeseries deletion has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as there is no output binding to be activated.
}

async function createTempTable (transaction, context) {
  // Create a local temporary table
  await new sql.Request(transaction).batch(`
      create table #delete_staging_data_temp
      (
        id uniqueidentifier not null default newid(),
        timeseries_id uniqueidentifier not null,
        timeseries_header_id uniqueidentifier not null,
        job_status int not null
      )
    `)
}

async function insertSoftDataIntoTemp (context, preparedStatement) {

  const SOFT_DELETION_DATE = process.env['SOFT_EXPIRY_LIMIT']
  const FME_COMPLETE_JOB_STATUS = process.env['FME_COMPLETE_JOB_STATUS']

  await preparedStatement.input('softDate', sql.DateTime2)
  await preparedStatement.input('jobStatus', sql.Int)

  await preparedStatement.prepare(
    `INSERT INTO ##r_timeseries_job_temp
    SELECT r.ID, r.TIMESERIES_ID, r.JOB_STATUS, t.TIMESERIES_HEADER_ID
    FROM [${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}].TIMESERIES_JOB r
      JOIN [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES t ON t.ID = r.TIMESERIES_ID
      JOIN [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES_HEADER h ON t.TIMESERIES_HEADER_ID = h.ID
    WHERE
      r.JOB_STATUS = @completeStatus
      AND
      h.IMPORT_TIME < CAST('@softDate' AS datetime2)`
  )

  const parameters = {
    softDate: SOFT_DELETION_DATE,
    jobStatus: FME_COMPLETE_JOB_STATUS
  }

  await preparedStatement.execute(parameters)
}

async function insertHardDataIntoTemp (context, preparedStatement) {
  const HARD_DELETION_DATE = process.env['HARD_EXPIRY_LIMIT']

  await preparedStatement.input('hardDate', sql.DateTime2)

  await preparedStatement.prepare(
    `INSERT INTO ##r_timeseries_job_temp
    SELECT r.ID, r.TIMESERIES_ID, r.JOB_STATUS, t.TIMESERIES_HEADER_ID
    FROM [${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}].TIMESERIES_JOB r
      JOIN [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES t ON t.ID = r.TIMESERIES_ID
      JOIN [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES_HEADER h ON t.TIMESERIES_HEADER_ID = h.ID
    WHERE
      h.IMPORT_TIME < CAST('@hardDate' AS datetime2)`
  )

  const parameters = {
    hardDate: HARD_DELETION_DATE
  }

  await preparedStatement.execute(parameters)
}

async function deleteReportingRows (context, preparedStatement) {

  await preparedStatement.prepare(
    `DELETE r FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB r
    INNER JOIN ##r_timeseries_job_temp te
    ON te.id = r.ID`
  )

  await preparedStatement.execute()
}

async function deleteTimeseriesRows (context, preparedStatement) {

  await preparedStatement.prepare(
    `DELETE t FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES t
    INNER JOIN ##delete_staging_data_temp te
    ON te.timeseries_id = t.id`
  )

  await preparedStatement.execute()
}

async function deleteHeaderRows (context, preparedStatement) {

  await preparedStatement.prepare(
    `DELETE th FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES_HEADER th
    INNER JOIN ##delete_staging_data_temp te
    ON te.timeseries_header_id = th.id`
  )

  await preparedStatement.execute()
}
