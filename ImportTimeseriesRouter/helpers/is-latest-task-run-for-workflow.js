const moment = require('moment')
const sql = require('mssql')

module.exports = async function isTaskRunImported (context, preparedStatement, routeData) {
  await preparedStatement.input('taskCompletionTime', sql.DateTime2)
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(`
    select top(1)
      task_id as latest_staged_task_id,
      task_completion_time as latest_staged_task_completion_time
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header
    where
      workflow_id = @workflowId and
      task_completion_time >= convert(datetime2, @taskCompletionTime, 126) at time zone 'UTC'
    order by
      task_completion_time desc
  `)

  const parameters = {
    taskCompletionTime: routeData.taskCompletionTime,
    workflowId: routeData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  if (result.recordset && result.recordset[0] && result.recordset[0].latest_staged_task_id) {
    routeData.latestTaskId = result.recordset[0].latest_staged_task_id
    routeData.latestTaskCompletionTime =
      moment(result.recordset[0].latest_staged_task_completion_time).toISOString()
  } else {
    routeData.latestTaskId = routeData.taskId
    routeData.latestTaskCompletionTime = routeData.taskCompletionTime
  }

  return routeData.latestTaskId === routeData.taskId
}
