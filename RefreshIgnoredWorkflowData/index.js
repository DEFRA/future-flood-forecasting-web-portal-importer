const refresh = require('../Shared/shared-refresh-csv-rows')

module.exports = async function (context, message) {
  const refreshData = {
    // Location of csv:
    csvUrl: process.env['IGNORED_WORKFLOW_URL'],
    workflowRefreshCsvType: 'I',
    // Destination table in staging database
    tableName: 'IGNORED_WORKFLOW',
    partialTableUpdate: { flag: false },
    // Column information and correspoding csv information
    functionSpecificData: [
      { tableColumnName: 'WORKFLOW_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'WorkflowID' }
    ],
    type: 'ignored workflow refresh'
  }

  await refresh(context, refreshData)
}
