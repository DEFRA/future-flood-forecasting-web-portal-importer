const refresh = require('../Shared/shared-refresh-csv-rows')

module.exports = async function (context, message) {
  const refreshData = {
    // Location of csv:
    csvUrl: process.env['COASTAL_MVT_FORECAST_LOCATION_URL'],
    // Destination table in staging database
    tableName: 'COASTAL_FORECAST_LOCATION',
    partialTableUpdate: { flag: true, whereClause: `where COASTAL_TYPE = 'Multivariate Thresholds'` },
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'FFFS_LOC_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
      { tableColumnName: 'COASTAL_ORDER', tableColumnType: 'Int', expectedCSVKey: 'CoastalOrder' },
      { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
      { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' },
      { tableColumnName: 'TA_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'TAName' },
      { tableColumnName: 'COASTAL_TYPE', tableColumnType: 'NVarChar', expectedCSVKey: 'Type' }
    ],
    type: 'mvt coastal location'
  }

  await refresh(context, refreshData)
}
