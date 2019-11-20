const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const { pooledConnect, sql } = require('../Shared/connection-pool')
const { doInTransaction } = require('../Shared/transaction-helper')

module.exports = async function (context, message) {
  async function refresh (transactionData) {
    await createLocationLookupTemporaryTable(new sql.Request(transactionData.transaction), context)
    await populateLocationLookupTemporaryTable(transactionData.preparedStatement, context)
    // Future requests will fail until the prepared statement is unprepared.
    await transactionData.preparedStatement.unprepare()
    await refreshLocationLookupTable(new sql.Request(transactionData.transaction), context)
  }

  // Ensure the connection pool is ready
  await pooledConnect

  // Refresh the data in the location lookup table within a transaction with a serializable isolation
  // level so that refresh is prevented if the location lookup table is in use. If the location lookup
  // table is in use and location lookup table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, sql.ISOLATION_LEVEL.SERIALIZABLE)

  sql.on('error', err => {
    context.log.error(err)
    throw err
  })
}

async function createLocationLookupTemporaryTable (request, context) {
  try {
    // Create a global temporary table to hold location lookup CSV data.
    await request.batch(`
      create table ##location_lookup_temp
      (
        id uniqueidentifier not null default newid(),
        workflow_id nvarchar(64) not null,
        plot_id nvarchar(64) not null,
        location_id nvarchar(64) not null
        constraint pk_location_lookup_temp primary key(id)
      )
    `)
  } catch (err) {
    context.log.error(err)
    throw err
  }
}

async function populateLocationLookupTemporaryTable (preparedStatement, context) {
  // Use the fetch API to retrieve the CSV data as a stream and then parse it
  // into rows ready for insertion into the global temporary table.
  const response = await fetch(`${process.env['LOCATION_LOOKUP_URL']}`)
  let rows = await neatCsv(response.body)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.input('plotId', sql.NVarChar)
  await preparedStatement.input('locationId', sql.NVarChar)
  await preparedStatement.prepare(`insert into ##location_lookup_temp (workflow_id, plot_id, location_id) values (@workflowId, @plotId, @locationId)`)

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
}

async function refreshLocationLookupTable (request, context) {
  const recordCountResponse = await request.query(`select count(*) as number from ##location_lookup_temp`)
  // Do not refresh the location lookup table if the global temporary table is empty.
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
            ##location_lookup_temp
          group by
            workflow_id,
            plot_id
      `)
  } else {
    context.log.warn('##location_lookup_temp contains no records - Aborting location_lookup refresh')
  }
  const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup`)
  context.log.info(`The location_lookup table contains ${result.recordset[0].number} records`)
}
