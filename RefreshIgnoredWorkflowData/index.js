const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const loadExceptions = require('../Shared/failed-csv-load-handler/load-csv-exceptions')
const refreshData = require('../Shared/shared-insert-csv-rows')
const sql = require('mssql')

module.exports = async function (context, message) {
  // Location of csv:
  const csvUrl = process.env['IGNORED_WORKFLOW_URL']
  // Destination table in staging database
  const tableName = 'IGNORED_WORKFLOW'
  const partialTableUpdate = { flag: false }
  // Column information and correspoding csv information
  const functionSpecificData = [
    { tableColumnName: 'WORKFLOW_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'WorkflowID' }
  ]

  let failedRows
  async function refresh (transaction, context) {
    failedRows = await executePreparedStatementInTransaction(refreshData, context, transaction, csvUrl, tableName, functionSpecificData, partialTableUpdate)
  }

  // Refresh with a serializable isolation level so that refresh is prevented if the ignored_workflow table is in use.
  // If the table is in use and table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The ignored workflow refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)

  // Transaction 2
  if (failedRows.length > 0) {
    await doInTransaction(loadExceptions, context, 'The ignored workflow exception load has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE, 'triton coastal locations', failedRows)
  } else {
    context.log.info(`There were no csv exceptions during load.`)
  }
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}
