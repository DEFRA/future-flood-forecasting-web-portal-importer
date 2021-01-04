const sql = require('mssql')

const countQueries = {
  coastal_display_group_workflow_temp: 'select count(*) as number from #coastal_display_group_workflow_temp',
  fluvial_display_group_workflow_temp: 'select count(*) as number from #fluvial_display_group_workflow_temp',
  fluvial_display_group_workflow: 'select count(*) as number from fff_staging.fluvial_display_group_workflow',
  coastal_display_group_workflow: 'select count(*) as number from fff_Staging.coastal_display_group_workflow'
}

const deleteAndTransferAggregatedRecordQueries = {
  fluvial_display_group_workflow: `
    delete from fff_staging.fluvial_display_group_workflow
    insert into fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
    select
      workflow_id,
      plot_id,
      string_agg(cast(location_id as nvarchar(max)), ';')
    from #fluvial_display_group_workflow_temp
    group by
      workflow_id,
      plot_id`,
  coastal_display_group_workflow: `
    delete from fff_staging.coastal_display_group_workflow
    insert into fff_staging.coastal_display_group_workflow (workflow_id, plot_id, location_ids)
    select
      workflow_id,
      plot_id,
      string_agg(cast(location_id as nvarchar(max)), ';')
    from #coastal_display_group_workflow_temp
    group by
      workflow_id,
      plot_id`
}

module.exports = async function refreshDisplayGroupTable (transaction, context, tempTableName, tableName) {
  try {
    let tempRecordCount = await countTableRecords(context, transaction, tempTableName, countQueries[tempTableName])
    // Do not refresh the display_group_workflow table if the local temporary table is empty.
    if (tempRecordCount > 0) {
      const request = new sql.Request(transaction)
      // Concatenate all locations for each combination of workflow ID and plot ID.
      await request.batch(deleteAndTransferAggregatedRecordQueries[tableName])
    } else {
      // If the csv is empty then the file is essentially ignored
      context.log.warn(`${tempTableName} contains no records - Aborting ${tableName} refresh`)
    }

    let recordCount = await countTableRecords(context, transaction, `fff_staging.${tableName}`, countQueries[tableName])
    if (recordCount === 0) {
      // If all the records in the csv (inserted into the temp table) are invalid, the function will overwrite records in the table with no new records
      // after the table has already been truncated. This function needs rolling back to avoid a blank database overwrite.
      // # The temporary table protects this from happening greatly reducing the likelihood of occurrence.
      context.log.warn(`There are no new records to insert into ${tableName}, rolling back refresh`)
      throw new Error('A null database overwrite is not allowed')
    }
  } catch (err) {
    context.log.error(`Refresh ${tableName} data failed: ${err}`)
    throw err
  }
}

async function countTableRecords (context, transaction, tableName, countQuery) {
  const request = new sql.Request(transaction)
  const result = await request.query(countQuery)
  context.log.info(`The ${tableName} table contains ${result.recordset[0].number} records`)
  return result.recordset[0].number
}
