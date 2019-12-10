const { doInTransaction } = require('../Shared/transaction-helper')
// const fetch = require('node-fetch')
// const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, message) {
  async function refresh (transactionData) {
    context.log.info('running')
  }

  // Refresh the data in the fluvial_display_group_workflow table within a transaction with a serializable isolation
  // level so that refresh is prevented if the fluvial_display_group_workflow table is in use. If the fluvial_display_group_workflow
  // table is in use and fluvial_display_group_workflow table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}
