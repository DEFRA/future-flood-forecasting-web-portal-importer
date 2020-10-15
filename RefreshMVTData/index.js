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
      { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre', nullValueOverride: true },
      { tableColumnName: 'CRITICAL_CONDITION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'criticalConditionID', nullValueOverride: true },
      { tableColumnName: 'INPUT_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'inputLocationID' },
      { tableColumnName: 'OUTPUT_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'outputLocationID' },
      { tableColumnName: 'TARGET_AREA_CODE', tableColumnType: 'NVarChar', expectedCSVKey: 'TargetAreaCode', nullValueOverride: true },
      { tableColumnName: 'INPUT_PARAMETER_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'inputParameterID' },
      { tableColumnName: 'LOWER_BOUND', tableColumnType: 'Decimal', expectedCSVKey: 'lowerBound', precision: 5, scale: 2, nullValueOverride: true, preprocessor: returnNullForNaN },
      { tableColumnName: 'UPPER_BOUND', tableColumnType: 'Decimal', expectedCSVKey: 'upperBound', precision: 5, scale: 2, nullValueOverride: true, preprocessor: returnNullForNaN },
      { tableColumnName: 'LOWER_BOUND_INCLUSIVE', tableColumnType: 'Bit', expectedCSVKey: 'lowerBoundInclusive', preprocessor: parseBooleanString, nullValueOverride: true },
      { tableColumnName: 'UPPER_BOUND_INCLUSIVE', tableColumnType: 'Bit', expectedCSVKey: 'upperBoundInclusive', preprocessor: parseBooleanString, nullValueOverride: true },
      { tableColumnName: 'PRIORITY', tableColumnType: 'Int', expectedCSVKey: 'Priority', nullValueOverride: true }
    ],
    type: 'mvt'
  }
  await refresh(context, refreshData)
}

function parseBooleanString (booleanString) {
  return JSON.parse(booleanString.toLowerCase())
}

function returnNullForNaN (value) {
  if (isNaN(value) || value === '') {
    return null
  } else {
    return value
  }
}
