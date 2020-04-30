const refresh = require('../Shared/shared-refresh-csv-rows')

module.exports = async function (context, message) {
  const refreshData = {
    // Location of csv:
    csvUrl: process.env['FLUVIAL_FORECAST_LOCATION_URL'],
    // Destination table in staging database
    tableName: 'FLUVIAL_FORECAST_LOCATION',
    partialTableUpdate: { flag: false },
    // Column information and correspoding csv information
    functionSpecificData: [
      { tableColumnName: 'FFFS_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
      { tableColumnName: 'FFFS_LOCATION_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName' },
      { tableColumnName: 'DRN_ORDER', tableColumnType: 'Int', expectedCSVKey: 'DRNOrder' },
      { tableColumnName: 'DATUM', tableColumnType: 'NVarChar', expectedCSVKey: 'Datum' },
      { tableColumnName: 'DISPLAY_ORDER', tableColumnType: 'Int', expectedCSVKey: 'Order' },
      { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
      { tableColumnName: 'PLOT_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'PlotID' },
      { tableColumnName: 'CATCHMENT', tableColumnType: 'NVarChar', expectedCSVKey: 'Catchment' },
      { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' }
    ],
    type: 'fluvial forecast location refresh'
  }
  await refresh(context, refreshData)
}
