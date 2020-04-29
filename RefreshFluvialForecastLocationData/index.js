const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const loadExceptions = require('../Shared/failed-csv-load-handler/load-csv-exceptions')
const refreshData = require('../Shared/shared-insert-csv-rows')
const sql = require('mssql')

module.exports = async function (context, message) {
  // Location of csv:
  const csvUrl = process.env['FLUVIAL_FORECAST_LOCATION_URL']
  // Destination table in staging database
  const tableName = 'FLUVIAL_FORECAST_LOCATION'
  const partialTableUpdate = { flag: false }
  // Column information and correspoding csv information
  const functionSpecificData = [
    { tableColumnName: 'FFFS_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
    { tableColumnName: 'FFFS_LOCATION_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName' },
    { tableColumnName: 'DRN_ORDER', tableColumnType: 'Int', expectedCSVKey: 'DRNOrder' },
    { tableColumnName: 'DATUM', tableColumnType: 'NVarChar', expectedCSVKey: 'Datum' },
    { tableColumnName: 'DISPLAY_ORDER', tableColumnType: 'Int', expectedCSVKey: 'Order' },
    { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
    { tableColumnName: 'PLOT_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'PlotID' },
    { tableColumnName: 'CATCHMENT', tableColumnType: 'NVarChar', expectedCSVKey: 'Catchment' },
    { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' }
  ]

  let failedRows
  async function refresh (transaction, context) {
    failedRows = await executePreparedStatementInTransaction(refreshData, context, transaction, csvUrl, tableName, functionSpecificData, partialTableUpdate)
  }

  // Refresh the data in the forecast location table within a transaction with a serializable isolation
  // level so that refresh is prevented if the forecast location table is in use. If the forecast location
  // table is in use and forecast location table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The fluvial_forecast_location refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)

  // Transaction 2
  if (failedRows.length > 0) {
    await doInTransaction(loadExceptions, context, 'The fluvial forecast location exception load has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE, 'triton coastal locations', failedRows)
  } else {
    context.log.info(`There were no csv exceptions during load.`)
  }
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}
