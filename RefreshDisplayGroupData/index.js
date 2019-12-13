const { doInTransaction } = require('../Shared/transaction-helper')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, message) {
  async function refresh (transactionData) {
    await createDisplayGroupTemporaryTable(new sql.Request(transactionData.transaction), context)
    await populateDisplayGroupTemporaryTable(transactionData.preparedStatement, context)
    await refreshDisplayGroupTable(new sql.Request(transactionData.transaction), context)
  }

  // Refresh the data in the fluvial_display_group_workflow table within a transaction with a serializable isolation
  // level so that refresh is prevented if the fluvial_display_group_workflow table is in use. If the fluvial_display_group_workflow
  // table is in use and fluvial_display_group_workflow table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The FLUVIAL_DISPLAY_GROUP_WORKFLOW refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}

async function createDisplayGroupTemporaryTable (request, context) {
  // Create a local temporary table to hold fluvial_display_group CSV data.
  await request.batch(`
      create table #fluvial_display_group_workflow_temp
      (
        id uniqueidentifier not null default newid(),
        workflow_id nvarchar(64) not null,
        plot_id nvarchar(64) not null,
        location_id nvarchar(64) not null
      )
    `)
}

async function populateDisplayGroupTemporaryTable (preparedStatement, context) {
  // The temp table provides two functions:
  // - a preliminary check of the csv data before it is inserted into the staging table
  // - the ability to aggregate location data in the fluvial_display_group_workflow table
  try {
    // Use the fetch API to retrieve the CSV data as a stream and then parse it
    // into rows ready for insertion into the local temporary table.
    const response = await fetch(`${process.env['FLUVIAL_DISPLAY_GROUP_WORKFLOW_URL']}`)
    const rows = await neatCsv(response.body)
    await preparedStatement.input('workflowId', sql.NVarChar)
    await preparedStatement.input('plotId', sql.NVarChar)
    await preparedStatement.input('locationId', sql.NVarChar)
    await preparedStatement.prepare(`insert into #fluvial_display_group_workflow_temp (workflow_id, plot_id, location_id) values (@workflowId, @plotId, @locationId)`)

    for (const row of rows) {
      // Ignore rows in the CSV data that do not have entries for all columns.
      if (row.WorkflowID && row.PlotID && row.FFFSLocID) {
        await preparedStatement.execute({
          workflowId: row.WorkflowID,
          plotId: row.PlotID,
          locationId: row.FFFSLocID
        })
      }
    }
    // Future requests will fail until the prepared statement is unprepared.
    await preparedStatement.unprepare()
  } catch (err) {
    context.log.error(`Populate temp location loookup table failed: ${err}`)
    throw err
  }
}

async function refreshDisplayGroupTable (request, context) {
  try {
    const recordCountResponse = await request.query(`select count(*) as number from #fluvial_display_group_workflow_temp`)
    // Check the local temporary table for records. If empty do not refresh the fluvial_display_group_workflow table.
    if (recordCountResponse.recordset[0].number > 0) {
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
      // Concatenate all locations for each combination of workflow ID and plot ID.
      await request.query(`
        insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
          select
            workflow_id,
            plot_id,
            string_agg(location_id, ';')
          from
            #fluvial_display_group_workflow_temp
          group by
            workflow_id,
            plot_id
      `)
    } else {
      // If the temp table is empty then the file is essentially ignored to prevent a blank database overwrite
      context.log.warn('#fluvial_display_group_workflow_temp contains no records - Aborting fluvial_display_group_workflow refresh')
    }
    const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
    context.log.info(`The fluvial_display_group_workflow table contains ${result.recordset[0].number} records`)
  } catch (err) {
    context.log.error(`Refresh fluvial_display_group_workflow data failed: ${err}`)
    throw err
  }
}
