const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')
const commonRefreshData = require('../Shared/csv-load/common-refresh-data')

const functionSpecificData = [
  { tableColumnName: 'LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'LOCATIONID' },
  { tableColumnName: 'THRESHOLD_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'ID' },
  { tableColumnName: 'GROUP_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'GROUPID'},
  { tableColumnName: 'NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'NAME' },
  { tableColumnName: 'LABEL', tableColumnType: 'NVarChar', expectedCSVKey: 'LABEL' },
  { tableColumnName: 'VALUE', tableColumnType: 'Decimal', expectedCSVKey: 'VALUE', precision: 38, scale: 8, nullValueOverride: true, preprocessor: commonRefreshData.returnNullForNaN },
  { tableColumnName: 'FLUVIAL_TYPE', tableColumnType: 'NVarChar', expectedCSVKey: 'FLUVIALTYPE' },
  { tableColumnName: 'COMMENT', tableColumnType: 'NVarChar', expectedCSVKey: 'COMMENT', nullValueOverride: true, preprocessor: returnNullForEmptyString },
  { tableColumnName: 'DESCRIPTION', tableColumnType: 'NVarChar', expectedCSVKey: 'DESCRIPTION' }
]

module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env.LOCATION_THRESHOLDS_URL,
    nonWorkflowRefreshCsvType: 'LTH',
    tableName: 'location_thresholds',
    csvSourceFile: 'location thresholds refresh',
    deleteStatement: 'delete from fff_staging.location_thresholds',
    countStatement: 'select count(*) as number from fff_staging.location_thresholds',
    insertPreparedStatement: `
      insert into 
        fff_staging.location_thresholds (location_id, threshold_id, name, label, value, group_id, fluvial_type, comment, description)
      values 
        (@location_id, @threshold_id, @name, @label, @value, @group_id, @fluvial_type, @comment, @description)`,
    // Column information and corresponding csv information
    functionSpecificData
  }

  await refresh(context, refreshData)

}

function returnNullForEmptyString (value) {
  if (value && value.length > 0) {
    return value
  }
  return null
}
