const sql = require('mssql')

module.exports = async function isTaskRunImported (context, preparedStatement, taskRunId) {
  await preparedStatement.input('taskRunId', sql.NVarChar)

  await preparedStatement.prepare(`
    select
      count(task_run_id) as task_run_id_count
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header
    where
      task_run_id = @taskRunId
  `)
  const parameters = {
    taskRunId: taskRunId
  }

  const result = await preparedStatement.execute(parameters)
  return result.recordset && result.recordset[0] && result.recordset[0].task_run_id_count > 0
}
