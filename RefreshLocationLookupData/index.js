const { doInTransaction } = require('../Shared/transaction-helper')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, message) {
  async function refresh (transactionData) {
    await createLocationLookupTemporaryTable(new sql.Request(transactionData.transaction), context)
    await populateLocationLookupTemporaryTable(transactionData.preparedStatement, context)
    await refreshLocationLookupTable(new sql.Request(transactionData.transaction), context)
  }

  // Refresh the data in the location lookup table within a transaction with a serializable isolation
  // level so that refresh is prevented if the location lookup table is in use. If the location lookup
  // table is in use and location lookup table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  try {
    await doInTransaction(refresh, context, sql.ISOLATION_LEVEL.SERIALIZABLE)
  } catch (err) {
    context.log.error(`Transaction failed: The location_lookup refresh has failed with the following error: ${err}`)
    throw err
  }

  sql.on('error', err => {
    context.log.error(err)
    throw err
  })
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}

async function createLocationLookupTemporaryTable (request, context) {
  // Create a local temporary table to hold location lookup CSV data.
  await request.batch(`
      create table #location_lookup_temp
      (
        id uniqueidentifier not null default newid(),
        workflow_id nvarchar(64) not null,
        plot_id nvarchar(64) not null,
        location_id nvarchar(64) not null
      )
    `)
}

async function populateLocationLookupTemporaryTable (preparedStatement, context) {
  try {
    // Use the fetch API to retrieve the CSV data as a stream and then parse it
    // into rows ready for insertion into the local temporary table.
    const response = await fetch(`${process.env['LOCATION_LOOKUP_URL']}`)
    const rows = await neatCsv(response.body)
    await preparedStatement.input('workflowId', sql.NVarChar)
    await preparedStatement.input('plotId', sql.NVarChar)
    await preparedStatement.input('locationId', sql.NVarChar)
    await preparedStatement.prepare(`insert into #location_lookup_temp (workflow_id, plot_id, location_id) values (@workflowId, @plotId, @locationId)`)

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

async function refreshLocationLookupTable (request, context) {
  try {
    const recordCountResponse = await request.query(`select count(*) as number from #location_lookup_temp`)
    // Do not refresh the location lookup table if the local temporary table is empty.
    if (recordCountResponse.recordset[0].number > 0) {
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup`)
      // Concatenate all locations for each combination of workflow ID and plot ID.
      await request.query(`
        insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup (workflow_id, plot_id, location_ids)
          select
            workflow_id,
            plot_id,
            string_agg(location_id, ';')
          from
            #location_lookup_temp
          group by
            workflow_id,
            plot_id
      `)
    } else {
      context.log.warn('#location_lookup_temp contains no records - Aborting location_lookup refresh')
    }
    const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup`)
    context.log.info(`The location_lookup table contains ${result.recordset[0].number} records`)
  } catch (err) {
    context.log.error(`Refresh location lookup data failed: ${err}`)
    throw err
  }
}
