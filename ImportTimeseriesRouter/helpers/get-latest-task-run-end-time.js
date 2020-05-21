const moment = require('moment')
const sql = require('mssql')

module.exports = async function isTaskRunImported (context, preparedStatement, routeData) {
  await preparedStatement.input('taskCompletionTime', sql.DateTime2)
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(`
    select top(1)
      task_run_id as previous_staged_task_run_id,
      task_completion_time as previous_staged_task_completion_time
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header
    where
      workflow_id = @workflowId 
    order by
      task_completion_time desc
  `)

  const parameters = {
    workflowId: routeData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  if (result.recordset && result.recordset[0] && result.recordset[0].previous_staged_task_run_id) {
    routeData.previousTaskRunId = result.recordset[0].previous_staged_task_run_id
    routeData.previousTaskCompletionTime =
      moment(result.recordset[0].previous_staged_task_completion_time).toISOString()
  } else {
    routeData.previousTaskCompletionTime = null // task run not yet present in db
  }

  return routeData
}
