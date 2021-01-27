const refreshDisplayGroupTable = require('../Shared/csv-load/display-group-helpers/refresh-display-group-data')
const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')
const sql = require('mssql')

module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env.FLUVIAL_DISPLAY_GROUP_WORKFLOW_URL,
    workflowRefreshCsvType: 'F',
    tableName: '#fluvial_display_group_workflow_temp',
    csvSourceFile: 'fluvial display group',
    deleteStatement: 'delete from fff_staging.#fluvial_display_group_workflow_temp',
    countStatement: 'select count(*) as number from fff_staging.#fluvial_display_group_workflow_temp',
    insertPreparedStatement: `
      insert into 
        fff_staging.#fluvial_display_group_workflow_temp (workflow_id, plot_id, location_id)   
      values
        (@workflow_id, @plot_id, @location_id)`,
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'workflow_id', tableColumnType: 'NVarChar', expectedCSVKey: 'WorkflowID' },
      { tableColumnName: 'plot_id', tableColumnType: 'NVarChar', expectedCSVKey: 'PlotID' },
      { tableColumnName: 'location_id', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' }
    ],
    preOperation: createDisplayGroupTemporaryTable,
    postOperation: refreshFromTempTable
  }

  await refresh(context, refreshData)
}

async function createDisplayGroupTemporaryTable (transaction, context) {
  // Ensure a local temporary table exists to hold fluvial_display_group CSV data.
  // Deletion of the local temporary table associated with the pooled database connection
  // is not managed by connection reset (see http://tediousjs.github.io/tedious/api-connection.html#function_reset)
  // as this appears to cause intermittent connection state problems.
  // Deletion of the local temporary table associated with the pooled database connection is performed manually.
  await new sql.Request(transaction).batch(`
    drop table if exists #fluvial_display_group_workflow_temp;
    create table #fluvial_display_group_workflow_temp
    (
      id uniqueidentifier not null default newid(),
      workflow_id nvarchar(64) not null,
      plot_id nvarchar(64) not null,
      location_id nvarchar(64) not null
    );
  `)
}

async function refreshFromTempTable (transaction, context) {
  const tempTableName = 'fluvial_display_group_workflow_temp'
  const tableName = 'fluvial_display_group_workflow'
  await refreshDisplayGroupTable(transaction, context, tempTableName, tableName)
}
