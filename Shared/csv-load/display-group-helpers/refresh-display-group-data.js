const sql = require('mssql')

const countQueries = {
  coastal_display_group_workflow_temp: 'select count(*) as number from #coastal_display_group_workflow_temp',
  fluvial_display_group_workflow_temp: 'select count(*) as number from #fluvial_display_group_workflow_temp',
  fluvial_display_group_workflow: 'select count(*) as number from fff_staging.fluvial_display_group_workflow',
  coastal_display_group_workflow: 'select count(*) as number from fff_Staging.coastal_display_group_workflow'
}

const deleteAndTransferAggregatedRecordQueries = {
  // Deletion of local temporary tables associated with pooled database connections
  // is not managed by connection reset (see http://tediousjs.github.io/tedious/api-connection.html#function_reset)
  // as this appears to cause intermittent connection state problems.
  // Deletion of local temporary tables associated with pooled database connections is performed manually.
  fluvial_display_group_workflow: `
    delete from fff_staging.fluvial_display_group_workflow;
    insert into fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
    select
      workflow_id,
      plot_id,
      string_agg(cast(location_id as nvarchar(max)), ';')
    from #fluvial_display_group_workflow_temp
    group by
      workflow_id,
      plot_id;
    drop table if exists #fluvial_display_group_workflow_temp;`,
  coastal_display_group_workflow: `
    delete from fff_staging.coastal_display_group_workflow;
    insert into fff_staging.coastal_display_group_workflow (workflow_id, plot_id, location_ids)
    select
      workflow_id,
      plot_id,
      string_agg(cast(location_id as nvarchar(max)), ';')
    from #coastal_display_group_workflow_temp
    group by
      workflow_id,
      plot_id;
    drop table if exists #coastal_display_group_workflow_temp;`
}

module.exports = async function refreshDisplayGroupTable (transaction, context, tempTableName, tableName) {
  try {
    const tempRecordCount = await countTableRecords(context, transaction, tempTableName, countQueries[tempTableName])
    context.log.info(`There are ${tempRecordCount} rows in the temp table. Now commencing concatenation of locations for each combination of workflow ID and plot ID.`)
    const request = new sql.Request(transaction)
    // Concatenate all locations for each combination of workflow ID and plot ID.
    await request.batch(deleteAndTransferAggregatedRecordQueries[tableName])

    const recordCount = await countTableRecords(context, transaction, `fff_staging.${tableName}`, countQueries[tableName])
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
