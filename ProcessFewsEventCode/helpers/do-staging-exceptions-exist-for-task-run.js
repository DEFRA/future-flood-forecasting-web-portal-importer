const sql = require('mssql')

module.exports = async function (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.prepare(`
    select
      id
    from
      fff_staging.staging_exception
    where
      task_run_id = @taskRunId
    `)

  const parameters = {
    taskRunId: taskRunData.taskRunId
  }

  const result = await preparedStatement.execute(parameters)

  return !!(result.recordset && result.recordset[0])
}
