const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')
const commonRefreshData = require('../Shared/csv-load/common-refresh-data')

const insertPreparedStatement = `
  insert into 
    fff_staging.multivariate_thresholds (centre, critical_condition_id, input_location_id, output_location_id, target_area_code,  input_parameter_id, lower_bound, upper_bound, lower_bound_inclusive, upper_bound_inclusive, priority) 
  values 
    (@centre, @critical_condition_id, @input_location_id, @output_location_id, @target_area_code, @input_parameter_id, @lower_bound,  @upper_bound, @lower_bound_inclusive, @upper_bound_inclusive, @priority)
`
module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env.MVT_URL,
    tableName: 'multivariate_thresholds',
    csvSourceFile: 'mvt',
    deleteStatement: 'delete from fff_staging.multivariate_thresholds',
    countStatement: 'select count(*) as number from fff_staging.multivariate_thresholds',
    insertPreparedStatement,
    // Column information, and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre', nullValueOverride: true },
      { tableColumnName: 'CRITICAL_CONDITION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'criticalConditionID', nullValueOverride: true },
      { tableColumnName: 'INPUT_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'inputLocationID' },
      { tableColumnName: 'OUTPUT_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'outputLocationID' },
      { tableColumnName: 'TARGET_AREA_CODE', tableColumnType: 'NVarChar', expectedCSVKey: 'TargetAreaCode', nullValueOverride: true },
      { tableColumnName: 'INPUT_PARAMETER_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'inputParameterID' },
      { tableColumnName: 'LOWER_BOUND', tableColumnType: 'Decimal', expectedCSVKey: 'lowerBound', precision: 5, scale: 2, nullValueOverride: true, preprocessor: commonRefreshData.returnNullForNaN },
      { tableColumnName: 'UPPER_BOUND', tableColumnType: 'Decimal', expectedCSVKey: 'upperBound', precision: 5, scale: 2, nullValueOverride: true, preprocessor: commonRefreshData.returnNullForNaN },
      { tableColumnName: 'LOWER_BOUND_INCLUSIVE', tableColumnType: 'Bit', expectedCSVKey: 'lowerBoundInclusive', preprocessor: parseBooleanString, nullValueOverride: true },
      { tableColumnName: 'UPPER_BOUND_INCLUSIVE', tableColumnType: 'Bit', expectedCSVKey: 'upperBoundInclusive', preprocessor: parseBooleanString, nullValueOverride: true },
      { tableColumnName: 'PRIORITY', tableColumnType: 'Int', expectedCSVKey: 'Priority', nullValueOverride: true }
    ]
  }
  await refresh(context, refreshData)
}

function parseBooleanString (booleanString) {
  return JSON.parse(booleanString.toLowerCase())
}
