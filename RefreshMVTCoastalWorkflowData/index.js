const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const loadExceptions = require('../Shared/failed-csv-load-handler/load-csv-exceptions')
const refreshData = require('../Shared/shared-insert-csv-rows')
const sql = require('mssql')

module.exports = async function (context, message) {
  // Location of csv:
  const csvUrl = process.env['COASTAL_MVT_WORKFLOW_URL']
  // Destination table in staging database
  const tableName = 'COASTAL_LOCATION'
  const partialTableUpdate = { flag: true, whereClause: `where COASTAL_TYPE = 'Multivariate Thresholds'` }
  // Column information and correspoding csv information
  const csvLoadData = [
    { tableColumnName: 'FFFS_LOC_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
    { tableColumnName: 'FFFS_LOC_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' }, // COPY FFFSLOCID TO ALLOW INSERT INTO THE TABLE - LocName does not exist in this csv.
    { tableColumnName: 'COASTAL_ORDER', tableColumnType: 'Int', expectedCSVKey: 'CoastalOrder' },
    { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
    { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' },
    { tableColumnName: 'TA_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'TAName' },
    { tableColumnName: 'COASTAL_TYPE', tableColumnType: 'NVarChar', expectedCSVKey: 'Type' }
  ]

  let failedRows
  async function refresh (transaction, context) {
    failedRows = await executePreparedStatementInTransaction(refreshData, context, transaction, csvUrl, tableName, csvLoadData, partialTableUpdate)
  }

  // Refresh with a serializable isolation level so that refresh is prevented if the coastal location table is in use.
  // If the table is in use and table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The ignored workflow refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)

  if (failedRows.length > 0) {
    await doInTransaction(loadExceptions, context, 'The mvt coastal location exception load has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE, 'mvt coastal locations', failedRows)
  } else {
    context.log.info(`There were no csv exceptions during load.`)
  }// context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}
