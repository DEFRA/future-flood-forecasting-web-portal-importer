const refreshDisplayGroupTable = require('./refresh-display-group-data')
const sql = require('mssql')

const COASTAL = 'coastal'
const FLUVIAL = 'fluvial'

module.exports = {
  getCoastalDisplayGroupMetadata: async function () {
    return await getDisplayGroupMetadata(process.env.COASTAL_DISPLAY_GROUP_WORKFLOW_URL, 'C', COASTAL)
  },

  getFluvialDisplayGroupMetadata: async function () {
    return await getDisplayGroupMetadata(process.env.FLUVIAL_DISPLAY_GROUP_WORKFLOW_URL, 'F', FLUVIAL)
  }
}

async function getDisplayGroupMetadata (csvUrl, workflowRefreshCsvType, displayGroupType) {
  return {
    csvUrl,
    workflowRefreshCsvType,
    refreshCsvTimeTable: 'workflow_refresh',
    tableName: `#${displayGroupType}_display_group_workflow_temp`,
    csvSourceFile: `${displayGroupType} display group`,
    deleteStatement: `delete from fff_staging.#${displayGroupType}_display_group_workflow_temp`,
    countStatement: `select count(*) as number from fff_staging.#${displayGroupType}_display_group_workflow_temp`,
    insertPreparedStatement: `
      insert into
        fff_staging.#${displayGroupType}_display_group_workflow_temp (workflow_id, plot_id, location_id)
      values 
        (@workflow_id, @plot_id, @location_id)`,
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'workflow_id', tableColumnType: 'NVarChar', expectedCSVKey: 'WorkflowID' },
      { tableColumnName: 'plot_id', tableColumnType: 'NVarChar', expectedCSVKey: 'PlotID' },
      { tableColumnName: 'location_id', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' }
    ],
    preOperation: displayGroupType === COASTAL ? createCoastalDisplayGroupTemporaryTable : createFluvialDisplayGroupTemporaryTable,
    postOperation: displayGroupType === COASTAL ? refreshFromCoastalTempTable : refreshFromFluvialTempTable
  }
}

async function createCoastalDisplayGroupTemporaryTable (transaction, context) {
  await createDisplayGroupTemporaryTable(transaction, context, COASTAL)
}

async function createFluvialDisplayGroupTemporaryTable (transaction, context) {
  await createDisplayGroupTemporaryTable(transaction, context, FLUVIAL)
}

async function createDisplayGroupTemporaryTable (transaction, context, displayGroupType) {
  // Ensure a local temporary table exists to hold coastal_display_group CSV data.
  // Deletion of the local temporary table associated with the pooled database connection
  // is not managed by connection reset (see http://tediousjs.github.io/tedious/api-connection.html#function_reset)
  // as this appears to cause intermittent connection state problems.
  // Deletion of the local temporary table associated with the pooled database connection is performed manually.
  await new sql.Request(transaction).batch(`
    drop table if exists #${displayGroupType}_display_group_workflow_temp;
    create table #${displayGroupType}_display_group_workflow_temp
    (
      id uniqueidentifier not null default newid(),
      workflow_id nvarchar(64) not null,
      plot_id nvarchar(64) not null,
      location_id nvarchar(64) not null
    );
  `)
}

async function refreshFromCoastalTempTable (transaction, context, displayGroupType) {
  await refreshFromTempTable(transaction, context, COASTAL)
}

async function refreshFromFluvialTempTable (transaction, context, displayGroupType) {
  await refreshFromTempTable(transaction, context, FLUVIAL)
}

async function refreshFromTempTable (transaction, context, displayGroupType) {
  const tempTableName = `${displayGroupType}_display_group_workflow_temp`
  const tableName = `${displayGroupType}_display_group_workflow`
  await refreshDisplayGroupTable(transaction, context, tempTableName, tableName)
}
