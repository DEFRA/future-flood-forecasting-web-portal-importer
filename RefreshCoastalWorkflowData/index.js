const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const refreshData = require('./shared-insert-csv-rows')
const sql = require('mssql')

module.exports = async function (context, message) {
  // needs extracting from service bus message!!! >>>
  const csvUrl = 'COASTAL_URL_Triton' // e.g = message.type
  // <<<

  const csvLoadData = [
    { tableColumnName: 'FFFS_LOC_ID', tableColumnType: sql.NVarchar, expectedCSVHeader: 'FFFSLocID' },
    { tableColumnName: 'COASTAL_ORDER', tableColumnType: sql.Int, expectedCSVHeader: 'CoastalOrder' },
    { tableColumnName: 'CENTRE', tableColumnType: sql.NVarchar, expectedCSVHeader: 'Centre' },
    { tableColumnName: 'MFDO_AREA', tableColumnType: sql.NVarchar, expectedCSVHeader: 'MFDOArea' },
    { tableColumnName: 'TA_NAME', tableColumnType: sql.NVarchar, expectedCSVHeader: 'TAName' },
    { tableColumnName: 'COASTAL_TYPE', tableColumnType: sql.NVarchar, expectedCSVHeader: 'Type' }
  ]

  const tableName = 'Coastal'

  async function refresh (transaction, context) {
    await executePreparedStatementInTransaction(refreshData, context, transaction, csvUrl, tableName, csvLoadData)
  }

  // Refresh with a serializable isolation level so that refresh is prevented if the ignored_workflow table is in use.
  // If the table is in use and table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The ignored workflow refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}
