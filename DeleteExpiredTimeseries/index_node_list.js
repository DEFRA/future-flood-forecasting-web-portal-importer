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
    // calculate the hard and soft dates before which records should be deleted
    // current date                     :-------------------------------------->|
    // soft date                        :---------------------|                  - delete all completed records before this date
    // hard date                        :------------|                           - delete all records before this date
    // delete only complete records here:            |--------|
    const HARD_DELETION_DATE = timeStamp.subtract(process.env['HARD_EXPIRY_LIMIT']).toISOString
    const SOFT_DELETION_DATE = timeStamp.subtract(process.env['SOFT_EXPIRY_LIMIT']).toISOString

    // collect all records to delete by id
    const deleteIds = {}
    deleteIds.completedIds = await executePreparedStatementInTransaction(gatherCompletedIds, context, transaction, SOFT_DELETION_DATE)
    deleteIds.expiredIds = await executePreparedStatementInTransaction(gatherExpiredIds, context, transaction, HARD_DELETION_DATE)

    // delete all records from tables conforming to referential integrity
    await executePreparedStatementInTransaction(deleteFromReporting, context, transaction, deleteIds.completed.reportingId, deleteIds.expired.reportingId)
    await executePreparedStatementInTransaction(deleteFromTimeseries, context, transaction, deleteIds.completed.timeseriesId, deleteIds.expired.timeseriesId)
    await executePreparedStatementInTransaction(deleteFromHeader, context, transaction, deleteIds.completed.headerId, deleteIds.expired.headerId)
  }

  // Refresh with a serializable isolation level so that record deletion is prevented if the tables are in use.
  // If the table is in use and table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, manual intervention will be required.
  await doInTransaction(removeExpiredTimeseries, context, 'The expired timeseries deletion has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as there is no output binding to be activated.
}

async function gatherCompletedIds (context, preparedStatement, SOFT_DELETION_DATE) {
  await preparedStatement.input('softDate', sql.DateTime2)
  await preparedStatement.input('hardDate', sql.DateTime2)

  await preparedStatement.prepare(
    `SELECT ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB.ID AS reportingID, ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES.ID AS timeseriesID, ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES_HEADER.ID AS headerID
      FROM [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES 
    JOIN ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB ON [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES.ID = ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB.TIMESERIES_ID
    JOIN [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES_HEADER ON [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES.TIMESERIES_HEADER_ID = ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES_HEADER.ID
      WHERE
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB.JOB_STATUS = 6 
      AND
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES_HEADER.IMPORT_TIME < CAST(@softDate AS datetime2)`
  )

  const parameters = {
    softDate: SOFT_DELETION_DATE
  }

  const completedIds = await preparedStatement.execute(parameters)
  return completedIds
}

async function gatherExpiredIds (context, preparedStatement, SOFT_DELETION_DATE, HARD_DELETION_DATE) {
  await preparedStatement.input('softDate', sql.DateTime2)
  await preparedStatement.input('hardDate', sql.DateTime2)

  await preparedStatement.prepare(
    `SELECT ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB.ID AS reportingID, ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES.ID AS timeseriesID, ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES_HEADER.ID AS headerID
      FROM [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES 
    JOIN ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB ON [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES.ID = ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB.TIMESERIES_ID
    JOIN [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES_HEADER ON [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].TIMESERIES.TIMESERIES_HEADER_ID = ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES_HEADER.ID
      WHERE
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES_HEADER.IMPORT_TIME < CAST(@hardDate AS datetime2)`
  )

  const parameters = {
    hardDate: HARD_DELETION_DATE
  }

  const expiredIds = await preparedStatement.execute(parameters)
  return expiredIds
}

async function deleteFromReporting (context, preparedStatement, completedReportingIds, expiredReportingIds) {
  // Combine Ids
  const idsToDelete = {}
  Object.assign(idsToDelete, completedReportingIds, expiredReportingIds)
  const commaSeperatedIdsToDeleteString = JSON.stringify(idsToDelete)
  // e.g '1d91fa4c-2232-41c7-99ce-5d7ae1804f41', '437a4cd7-af4a-42c0-be83-190531f2861e'

  await preparedStatement.input('ids')

  await preparedStatement.prepare(
    `
    DELETE 
      FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.TIMESERIES_JOB 
    WHERE ID IN 
    (@ids)

    DELETE 
    FROM FFFS_REPORTING.TIMESERIES_JOB
    WHERE EXISTS(
    SELECT FFFS_REPORTING.TIMESERIES_JOB.ID AS reportingID, FFFS_STAGING.TIMESERIES.ID AS timeseriesID, FFFS_STAGING.TIMESERIES_HEADER.ID AS headerID
        FROM [FFFS_STAGING].TIMESERIES
        JOIN FFFS_REPORTING.TIMESERIES_JOB ON [FFFS_STAGING].TIMESERIES.ID = FFFS_REPORTING.TIMESERIES_JOB.TIMESERIES_ID
        JOIN [FFFS_STAGING].TIMESERIES_HEADER ON [FFFS_STAGING].TIMESERIES.TIMESERIES_HEADER_ID = FFFS_STAGING.TIMESERIES_HEADER.ID
    WHERE
        FFFS_REPORTING.TIMESERIES_JOB.JOB_STATUS = 6
    AND
        FFFS_STAGING.TIMESERIES_HEADER.IMPORT_TIME < CAST('2025-01-28' AS datetime2)
    )
    `
  )

  const parameters = {
    ids: commaSeperatedIdsToDeleteString
  }

  await preparedStatement.execute(parameters)
}

async function deleteFromTimeseries (context, preparedStatement, completedTimeseriesIds, expiredTimeseriesIds) {
  // Combine Ids
  const idsToDelete = {}
  Object.assign(idsToDelete, completedTimeseriesIds, expiredTimeseriesIds)
  const commaSeperatedIdsToDeleteString = JSON.stringify(idsToDelete)
  // e.g '1d91fa4c-2232-41c7-99ce-5d7ae1804f41', '437a4cd7-af4a-42c0-be83-190531f2861e'

  await preparedStatement.input('ids')

  await preparedStatement.prepare(
    `
    DELETE 
      FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES
    WHERE EXISTS ( SELECT *
                  FROM [fffswebportalstaging].[FFFS_REPORTING].[TIMESERIES_JOB]
                  WHERE JOB_ID = 8 ${idsToDelete.forEach('AND '+ )} JOB_ID = 7
    `
  )

  const parameters = {
    ids: commaSeperatedIdsToDeleteString
  }

  await preparedStatement.execute(parameters)
}

async function deleteFromHeader (context, preparedStatement, completedHeaderIds, expiredHeaderIds) {
  // Combine Ids
  const idsToDelete = {}
  Object.assign(idsToDelete, completedHeaderIds + expiredHeaderIds)
  const commaSeperatedIdsToDeleteString = JSON.stringify(idsToDelete)
  // e.g '1d91fa4c-2232-41c7-99ce-5d7ae1804f41', '437a4cd7-af4a-42c0-be83-190531f2861e'

  await preparedStatement.input('ids')

  await preparedStatement.prepare(
    `
    DELETE 
      FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.TIMESERIES_HEADER
    WHERE ID IN 
    (@ids)
    `
  )

  const parameters = {
    ids: commaSeperatedIdsToDeleteString
  }

  await preparedStatement.execute(parameters)
}
