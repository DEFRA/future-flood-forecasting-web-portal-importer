const refresh = require('../Shared/shared-refresh-csv-rows')

module.exports = async function (context, message) {
  const refreshData = {
    // Location of csv:
    csvUrl: process.env['COASTAL_TIDAL_FORECAST_LOCATION_URL'],
    // Destination table in staging database
    tableName: 'COASTAL_FORECAST_LOCATION',
    partialTableUpdate: { flag: true, whereClause: `where COASTAL_TYPE = 'Coastal Forecasting'` },
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'FFFS_LOC_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
      { tableColumnName: 'FFFS_LOC_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName' },
      { tableColumnName: 'COASTAL_ORDER', tableColumnType: 'Int', expectedCSVKey: 'CoastalOrder' },
      { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
      { tableColumnName: 'COASTAL_TYPE', tableColumnType: 'NVarChar', expectedCSVKey: 'Type' }
    ],
    type: 'tidal coastal location'
  }

  await refresh(context, refreshData)
}
