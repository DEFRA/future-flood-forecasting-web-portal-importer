const refresh = require('../Shared/shared-refresh-csv-rows')

module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env.IGNORED_WORKFLOW_URL,
    workflowRefreshCsvType: 'I',
    tableName: 'ignored_workflow',
    csvSourceFile: 'ignored workflow refresh',
    deleteStatement: 'delete from fff_staging.ignored_workflow',
    countStatement: 'select count(*) as number from fff_staging.ignored_workflow',
    insertPreparedStatement: `
    insert into 
      fff_staging.ignored_workflow (workflow_id)
    values 
      (@workflow_id)`,
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'WORKFLOW_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'WorkflowID' }
    ]
  }

  await refresh(context, refreshData)
}
