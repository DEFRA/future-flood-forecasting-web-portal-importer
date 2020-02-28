const sql = require('mssql')

module.exports = async function isTaskRunImported (context, preparedStatement, taskId) {
  await preparedStatement.input('taskId', sql.NVarChar)

  await preparedStatement.prepare(`
    select
      count(task_id) as task_id_count
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header
    where
      task_id = @taskId
  `)
  const parameters = {
    taskId: taskId
  }

  const result = await preparedStatement.execute(parameters)
  return result.recordset && result.recordset[0] && result.recordset[0].task_id_count > 0
}
