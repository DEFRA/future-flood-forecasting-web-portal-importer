import { executePreparedStatementInTransaction } from '../transaction-helper.js'
import { addLatestTaskRunCompletionPropertiesFromQueryResultToTaskRunData } from '../utils.js'
import sql from 'mssql'

export default async function (context, taskRunData) {
  return Promise.resolve(await executePreparedStatementInTransaction(isLatestTaskRunForWorkflow, context, taskRunData.transaction, taskRunData))
}

async function isLatestTaskRunForWorkflow (context, preparedStatement, taskRunData) {
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
  addLatestTaskRunCompletionPropertiesFromQueryResultToTaskRunData(taskRunData, result)
  return taskRunData.latestTaskRunId === taskRunData.taskRunId
}
