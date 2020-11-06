const transferAggregatedRecords = require('../Shared/transfer-aggregated-records')
const countTableRecords = require('../Shared/count-table-records')
const refresh = require('../Shared/shared-refresh-csv-rows')
const sql = require('mssql')

module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env['COASTAL_DISPLAY_GROUP_WORKFLOW_URL'],
    workflowRefreshCsvType: 'C',
    tableName: '#coastal_display_group_workflow_temp',
    csvSourceFile: 'coastal display group',
    deleteStatement: 'delete from fff_staging.#coastal_display_group_workflow_temp',
    countStatement: 'select count(*) as number from fff_staging.#coastal_display_group_workflow_temp',
    insertPreparedStatement: `
      insert into 
        fff_staging.#coastal_display_group_workflow_temp (workflow_id, plot_id, location_id)
      values 
        (@workflow_id, @plot_id, @location_id)`,
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'workflow_id', tableColumnType: 'NVarChar', expectedCSVKey: 'WorkflowID' },
      { tableColumnName: 'plot_id', tableColumnType: 'NVarChar', expectedCSVKey: 'PlotID' },
      { tableColumnName: 'location_id', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' }
    ],
    preOperation: createDisplayGroupTemporaryTable,
    postOperation: refreshDisplayGroupTable
  }

  await refresh(context, refreshData)
}

async function createDisplayGroupTemporaryTable (transaction, context) {
  // Create a local temporary table to hold coastal_display_group CSV data.
  await new sql.Request(transaction).batch(`
      create table #coastal_display_group_workflow_temp
      (
        id uniqueidentifier not null default newid(),
        workflow_id nvarchar(64) not null,
        plot_id nvarchar(64) not null,
        location_id nvarchar(64) not null
      )
  `)
}

async function refreshDisplayGroupTable (transaction, context) {
  try {
    let tempRecordCount = await countTableRecords(context, transaction, '#coastal_display_group_workflow_temp')
    // Do not refresh the coastal_display_group_workflow table if the local temporary table is empty.
    if (tempRecordCount > 0) {
      await transferAggregatedRecords(context, transaction, '#coastal_display_group_workflow_temp', 'fff_staging.coastal_display_group_workflow')
    } else {
      // If the csv is empty then the file is essentially ignored
      context.log.warn('#coastal_display_group_workflow_temp contains no records - Aborting coastal_display_group_workflow refresh')
    }

    let recordCount = await countTableRecords(context, transaction, 'fff_staging.coastal_display_group_workflow')
    if (recordCount === 0) {
      // If all the records in the csv (inserted into the temp table) are invalid, the function will overwrite records in the table with no new records
      // after the table has already been truncated. This function needs rolling back to avoid a blank database overwrite.
      // # The temporary table protects this from happening greatly reducing the likelihood of occurance.
      context.log.warn('There are no new records to insert into coastal_display_group_workflow_temp, rolling back refresh')
      throw new Error('A null database overwrite is not allowed')
    }
  } catch (err) {
    context.log.error(`Refresh coastal_display_group_workflow data failed: ${err}`)
    throw err
  }
}
