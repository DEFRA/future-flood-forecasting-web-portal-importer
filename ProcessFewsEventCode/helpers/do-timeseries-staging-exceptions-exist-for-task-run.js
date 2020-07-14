const sql = require('mssql')

module.exports = async function (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.prepare(`
    select
      tse.id
    from
      fff_staging.timeseries_header th,
      fff_staging.timeseries_staging_exception tse
    where
      th.task_run_id = @taskRunId and
      th.id = tse.timeseries_header_id
    `)

  const parameters = {
    taskRunId: taskRunData.taskRunId
  }

  const result = await preparedStatement.execute(parameters)

  return !!(result.recordset && result.recordset[0])
}
