const refresh = require('../Shared/shared-refresh-csv-rows')

module.exports = async function (context, message) {
  const refreshData = {
    // Location of csv:
    csvUrl: process.env['MVT_URL'],
    // Destination table in staging database
    tableName: 'MULTIVARIATE_THRESHOLDS',
    partialTableUpdate: { flag: false },
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
      { tableColumnName: 'CRITICAL_CONDITION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'criticalConditionID' },
      { tableColumnName: 'INPUT_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'inputLocationID' },
      { tableColumnName: 'OUTPUT_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'outputLocationID' },
      { tableColumnName: 'TARGET_AREA_CODE', tableColumnType: 'NVarChar', expectedCSVKey: 'TargetAreaCode' },
      { tableColumnName: 'INPUT_PARAMETER_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'inputParameterID' },
      { tableColumnName: 'LOWER_BOUND', tableColumnType: 'Float', expectedCSVKey: 'lowerBound' },
      { tableColumnName: 'UPPER_BOUND', tableColumnType: 'Float', expectedCSVKey: 'upperBound' },
      { tableColumnName: 'LOWER_BOUND_INCLUSIVE', tableColumnType: 'Bit', expectedCSVKey: 'lowerBoundInclusive', preprocessor: parseBooleanString },
      { tableColumnName: 'UPPER_BOUND_INCLUSIVE', tableColumnType: 'Bit', expectedCSVKey: 'upperBoundInclusive', preprocessor: parseBooleanString },
      { tableColumnName: 'PRIORITY', tableColumnType: 'Int', expectedCSVKey: 'Priority' }
    ],
    type: 'mvt'
  }
  await refresh(context, refreshData)
}

function parseBooleanString (booleanString) {
  return JSON.parse(booleanString.toLowerCase())
}
