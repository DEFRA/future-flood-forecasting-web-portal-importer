const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const loadExceptions = require('../Shared/failed-csv-load-handler/load-csv-exceptions')
const tempTableInsert = require('../Shared/shared-insert-csv-rows')
const sql = require('mssql')

module.exports = async function (context, message) {
  // Location of csv:
  const csvUrl = process.env['COASTAL_DISPLAY_GROUP_WORKFLOWS_URL']
  // Destination table in staging database
  const tableName = '#coastal_display_group_workflow_temp'
  const partialTableUpdate = { flag: false }
  // Column information and correspoding csv information
  const functionSpecificData = [
    { tableColumnName: 'workflow_id', tableColumnType: 'NVarChar', expectedCSVKey: 'WorkflowID' },
    { tableColumnName: 'plot_id', tableColumnType: 'NVarChar', expectedCSVKey: 'PlotID' },
    { tableColumnName: 'location_id', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' }
  ]

  let failedRows
  async function refresh (transaction, context) {
    await createDisplayGroupTemporaryTable(transaction, context)
    failedRows = await executePreparedStatementInTransaction(tempTableInsert, context, transaction, csvUrl, tableName, functionSpecificData, partialTableUpdate)
    if (!transaction._rollbackRequested) {
      await refreshDisplayGroupTable(transaction, context)
    }
  }

  // Refresh the data in the coastal_display_group_workflow table within a transaction with a serializable isolation
  // level so that refresh is prevented if the coastal_display_group_workflow table is in use. If the coastal_display_group_workflow
  // table is in use and coastal_display_group_workflow table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The COASTAL_DISPLAY_GROUP_WORKFLOW refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)

  // Transaction 2
  if (failedRows.length > 0) {
    await doInTransaction(loadExceptions, context, 'The tidal coastal location exception load has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE, 'tidal coastal locations', failedRows)
  } else {
    context.log.info(`There were no csv exceptions during load.`)
  }
  context.done() // not requried as the async function returns the desired result, there is no output binding to be activated.
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
    const recordCountResponse = await new sql.Request(transaction).query(`select count(*) as number from #coastal_display_group_workflow_temp`)
    // Do not refresh the coastal_display_group_workflow table if the local temporary table is empty.
    if (recordCountResponse.recordset[0].number > 0) {
      await new sql.Request(transaction).query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_display_group_workflow`)
      // Concatenate all locations for each combination of workflow ID and plot ID.
      await new sql.Request(transaction).query(`
        insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_display_group_workflow (workflow_id, plot_id, fffs_loc_ids)
          select
            workflow_id,
            plot_id,
            string_agg(cast(location_id as NVARCHAR(MAX)), ';')
          from
            #coastal_display_group_workflow_temp
          group by
            workflow_id,
            plot_id
      `)
    } else {
      // If the csv is empty then the file is essentially ignored
      context.log.warn('#coastal_display_group_workflow_temp contains no records - Aborting coastal_display_group_workflow refresh')
    }
    const result = await new sql.Request(transaction).query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_display_group_workflow`)
    context.log.info(`The coastal_display_group_workflow table contains ${result.recordset[0].number} records`)
    if (result.recordset[0].number === 0) {
      // If all the records in the csv (inserted into the temp table) are invalid, the function will overwrite records in the table with no new records
      // after the table has already been truncated. This function needs rolling back to avoid a blank database overwrite.
      // # The temporary table protects this from happening, greatly reducing the likelihood of occurance.
      context.log.warn('There are no new records to insert, rolling back coastal_display_group_workflow refresh')
      throw new Error('A null database overwrite is not allowed')
    }
  } catch (err) {
    context.log.error(`Refresh coastal_display_group_workflow data failed: ${err}`)
    throw err
  }
}
