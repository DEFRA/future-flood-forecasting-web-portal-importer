const refresh = require('../Shared/shared-refresh-csv-rows')
const { isBoolean } = require('../Shared/utils')

module.exports = async function (context, message) {
  const refreshData = {
    // Location of csv:
    csvUrl: process.env['NON_DISPLAY_GROUP_WORKFLOW_URL'],
    // Destination table in staging database
    tableName: 'NON_DISPLAY_GROUP_WORKFLOW',
    partialTableUpdate: { flag: false },
    // Column information and correspoding csv information
    functionSpecificData: [
      { tableColumnName: 'WORKFLOW_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'WorkflowID' },
      { tableColumnName: 'FILTER_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FilterID' },
      { tableColumnName: 'APPROVED', tableColumnType: 'Bit', expectedCSVKey: 'Approved', preprocessor: parseBooleanString },
      { tableColumnName: 'FORECAST', tableColumnType: 'Bit', expectedCSVKey: 'Forecast', preprocessor: parseBooleanString },
      { tableColumnName: 'START_TIME_OFFSET_HOURS', tableColumnType: 'Int', expectedCSVKey: 'StartTimeOffsetHours', nullValueOverride: true },
      { tableColumnName: 'END_TIME_OFFSET_HOURS', tableColumnType: 'Int', expectedCSVKey: 'EndTimeOffsetHours', nullValueOverride: true }
    ],
    type: 'non display group refresh',
    keyInteregator
  }

  await refresh(context, refreshData)
}

async function keyInteregator (rowKey, rowValue) {
  if (rowKey !== 'Approved' && rowKey !== 'Forecast') {
    return true
  } else if (typeof (rowValue) === 'string' && isBoolean(rowValue)) {
    return true
  } else {
    return false
  }
}

function parseBooleanString (booleanString) {
  return JSON.parse(booleanString.toLowerCase())
}
