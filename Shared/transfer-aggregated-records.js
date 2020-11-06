const sql = require('mssql')

module.exports = async function transferAggregatedRecords (context, transaction, sourceTableName, destinationTableName) {
  const request = new sql.Request(transaction)
  await request.input('sourceTable', sql.NVarChar, sourceTableName)
  await request.input('destinationTable', sql.NVarChar, destinationTableName)
  await request.batch(`EXEC('delete from ' + @destinationTable)`)
  // Concatenate all locations for each combination of workflow ID and plot ID.
  await request.batch(`
  EXEC(
    'insert into ' + @destinationTable + ' (workflow_id, plot_id, location_ids)
      select
        workflow_id,
        plot_id,
        string_agg(cast(location_id as NVARCHAR(MAX)), '';'')
      from ' +
        @sourceTable +
      ' group by
        workflow_id,
        plot_id'
  )
  `)
}

