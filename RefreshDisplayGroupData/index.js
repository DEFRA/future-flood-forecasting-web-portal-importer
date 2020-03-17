const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const createCSVStagingException = require('../Shared/create-csv-staging-exception')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, message) {
  async function refresh (transaction, context) {
    await createDisplayGroupTemporaryTable(transaction, context)
    await executePreparedStatementInTransaction(populateDisplayGroupTemporaryTable, context, transaction)
    await refreshDisplayGroupTable(transaction, context)
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

async function populateDisplayGroupTemporaryTable (context, preparedStatement) {
  const failedRows = []
  const transaction = preparedStatement.parent
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
      try {
        // Ignore rows in the CSV data that do not have entries for all columns.
        if (row.WorkflowID && row.PlotID && row.FFFSLocID) {
          await preparedStatement.execute({
            workflowId: row.WorkflowID,
            plotId: row.PlotID,
            locationId: row.FFFSLocID
          })
        } else {
          const failedRowInfo = {
            rowData: row,
            errorMessage: `A row is missing data.`,
            errorCode: `NA`
          }
          failedRows.push(failedRowInfo)
        }
      } catch (err) {
        context.log.warn(`an error has been found in a row with the Workflow ID: ${row.WorkflowID}.\n  Error : ${err}`)
        const failedRowInfo = {
          rowData: row,
          errorMessage: err.message,
          errorCode: err.code
        }
        failedRows.push(failedRowInfo)
      }
    }
    // Future requests will fail until the prepared statement is unprepared.
    await preparedStatement.unprepare()

    for (let i = 0; i < failedRows.length; i++) {
      await executePreparedStatementInTransaction(
        createCSVStagingException, // function
        context, // context
        transaction, // transaction
        `Display group data`, // args - csv file
        failedRows[i].rowData, // args - row data
        failedRows[i].errorMessage // args - error description
      )
    }
    context.log.error(`The display group csv loader has ${failedRows.length} failed row inserts.`)
  } catch (err) {
    context.log.error(`Populate temp location loookup table failed: ${err}`)
    throw err
  }
}

async function refreshDisplayGroupTable (transaction, context) {
  try {
    const recordCountResponse = await new sql.Request(transaction).query(`select count(*) as number from #fluvial_display_group_workflow_temp`)
    // Do not refresh the fluvial_display_group_workflow table if the local temporary table is empty.
    if (recordCountResponse.recordset[0].number > 0) {
      await new sql.Request(transaction).query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
      // Concatenate all locations for each combination of workflow ID and plot ID.
      await new sql.Request(transaction).query(`
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
      // If the csv is empty then the file is essentially ignored
      context.log.warn('#fluvial_display_group_workflow_temp contains no records - Aborting fluvial_display_group_workflow refresh')
    }
    const result = await new sql.Request(transaction).query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
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
