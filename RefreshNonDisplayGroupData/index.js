const refresh = require('../Shared/shared-refresh-csv-rows')
const { isBoolean } = require('../Shared/utils')

module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env.NON_DISPLAY_GROUP_WORKFLOW_URL,
    workflowRefreshCsvType: 'N',
    tableName: 'non_display_group_workflow',
    csvSourceFile: 'non display group refresh',
    deleteStatement: 'delete from fff_staging.non_display_group_workflow',
    countStatement: 'select count(*) as number from fff_staging.non_display_group_workflow',
    insertPreparedStatement: `
      insert into 
        fff_staging.non_display_group_workflow (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)  
      values 
        (@workflow_id, @filter_id, @approved, @start_time_offset_hours, @end_time_offset_hours, @timeseries_type)`,
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'WORKFLOW_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'WorkflowID' },
      { tableColumnName: 'FILTER_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FilterID' },
      { tableColumnName: 'APPROVED', tableColumnType: 'Bit', expectedCSVKey: 'Approved', preprocessor: parseBooleanString },
      { tableColumnName: 'START_TIME_OFFSET_HOURS', tableColumnType: 'Int', expectedCSVKey: 'StartTimeOffsetHours', nullValueOverride: true },
      { tableColumnName: 'END_TIME_OFFSET_HOURS', tableColumnType: 'Int', expectedCSVKey: 'EndTimeOffsetHours', nullValueOverride: true },
      { tableColumnName: 'TIMESERIES_TYPE', tableColumnType: 'NVarChar', expectedCSVKey: 'TimeSeriesType' }
    ]
  }

  await refresh(context, refreshData)
}

function parseBooleanString (booleanString, columnName) {
  if (typeof (booleanString) === 'string' && isBoolean(booleanString)) {
    return JSON.parse(booleanString.toLowerCase())
  } else {
    // throw an error to create a csv exception for this csv row
    throw new Error(`Csv data: '${booleanString}', for column: '${columnName}' is not of type Boolean.`)
  }
}
