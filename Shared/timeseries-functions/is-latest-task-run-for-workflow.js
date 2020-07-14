const moment = require('moment')
const sql = require('mssql')

module.exports = async function (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunCompletionTime', sql.DateTime2)
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(`
    select top(1)
      task_run_id as latest_staged_task_run_id,
      task_completion_time as latest_staged_task_completion_time
    from
      fff_staging.timeseries_header
    where
      workflow_id = @workflowId and
      task_completion_time >= convert(datetime2, @taskRunCompletionTime, 126) at time zone 'UTC'
    order by
      task_completion_time desc
  `)

  const parameters = {
    taskRunCompletionTime: taskRunData.taskRunCompletionTime,
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  if (result.recordset && result.recordset[0] && result.recordset[0].latest_staged_task_run_id) {
    taskRunData.latestTaskRunId = result.recordset[0].latest_staged_task_run_id
    taskRunData.latestTaskRunCompletionTime =
      moment(result.recordset[0].latest_staged_task_completion_time).toISOString()
  } else {
    taskRunData.latestTaskRunId = taskRunData.taskRunId
    taskRunData.latestTaskRunCompletionTime = taskRunData.taskRunCompletionTime
  }

  return taskRunData.latestTaskRunId === taskRunData.taskRunId
}
