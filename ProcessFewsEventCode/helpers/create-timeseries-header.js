const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(createTimeseriesHeader, context, taskRunData.transaction, taskRunData)
}

async function createTimeseriesHeader (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunStartTime', sql.DateTime2)
  await preparedStatement.input('taskRunCompletionTime', sql.DateTime2)
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.input('forecast', sql.Bit)
  await preparedStatement.input('approved', sql.Bit)
  await preparedStatement.input('message', sql.NVarChar)

  await preparedStatement.prepare(`
    insert into
      fff_staging.timeseries_header
        (task_start_time, task_completion_time, task_run_id, workflow_id, forecast, approved, message)
    output
      inserted.id
    values
      (@taskRunStartTime, @taskRunCompletionTime, @taskRunId, @workflowId, @forecast, @approved, @message)
  `)

  const parameters = {
    taskRunStartTime: taskRunData.taskRunStartTime,
    taskRunCompletionTime: taskRunData.taskRunCompletionTime,
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId,
    forecast: taskRunData.forecast,
    approved: taskRunData.approved,
    message: taskRunData.message
  }

  await preparedStatement.execute(parameters)
  context.log(`Created timeseries header for ${taskRunData.taskRunId}`)
}
