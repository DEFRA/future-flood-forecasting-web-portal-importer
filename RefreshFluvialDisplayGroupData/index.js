const refresh = require('../Shared/shared-refresh-csv-rows')
const sql = require('mssql')

module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env['FLUVIAL_DISPLAY_GROUP_WORKFLOW_URL'],
    workflowRefreshCsvType: 'F',
    tableName: '#fluvial_display_group_workflow_temp',
    deleteStatement: 'delete from fff_staging.#fluvial_display_group_workflow_temp',
    countStatement: 'select count(*) as number from fff_staging.#fluvial_display_group_workflow_temp',
    insertPreparedStatement: `
      insert into 
        fff_staging.#fluvial_display_group_workflow_temp (workflow_id, plot_id, location_id)   
      values
        (@workflow_id, @plot_id, @location_id)`,
    // Column information and correspoding csv information
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
  // Create a local temporary table to hold fluvial_display_group CSV data.
  await new sql.Request(transaction).batch(`
      create table #fluvial_display_group_workflow_temp
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
    const recordCountResponse = await new sql.Request(transaction).query(`
    select 
      count(*) 
    as 
      number 
    from 
      #fluvial_display_group_workflow_temp`)
    // Do not refresh the fluvial_display_group_workflow table if the local temporary table is empty.
    if (recordCountResponse.recordset[0].number > 0) {
      await new sql.Request(transaction).query(`delete from fff_staging.fluvial_display_group_workflow`)
      // Concatenate all locations for each combination of workflow ID and plot ID.
      await new sql.Request(transaction).query(`
        insert into fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
          select
            workflow_id,
            plot_id,
            string_agg(cast(location_id as NVARCHAR(MAX)), ';')
          from
            #fluvial_display_group_workflow_temp
          group by
            workflow_id,
            plot_id
      `)
    } else {
      // If the csv is empty then the file is essentially ignored
      context.log.warn('#fluvial_display_group_workflow_temp contains no records - Aborting fluvial_display_group_workflow refresh')
    }
    const result = await new sql.Request(transaction).query(`
    select 
      count(*) 
    as 
      number 
    from 
      fff_staging.fluvial_display_group_workflow`)
    context.log.info(`The fluvial_display_group_workflow table contains ${result.recordset[0].number} records`)
    if (result.recordset[0].number === 0) {
      // If all the records in the csv (inserted into the temp table) are invalid, the function will overwrite records in the table with no new records
      // after the table has already been truncated. This function needs rolling back to avoid a blank database overwrite.
      // # The temporary table protects this from happening greatly reducing the likelihood of occurance.
      context.log.warn('There are no new records to insert, rolling back fluvial_display_group_workflow refresh')
      throw new Error('A null database overwrite is not allowed')
    }
  } catch (err) {
    context.log.error(`Refresh fluvial_display_group_workflow data failed: ${err}`)
    throw err
  }
}
