const doStagingExceptionsExistForTaskRun = require('../../Shared/timeseries-functions/do-staging-exceptions-exist-for-task-run')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  let ignoreMessage = false
  const timeseriesHeaderExistsForTaskRun =
    await executePreparedStatementInTransaction(doesTimeseriesHeaderExistForTaskRun, context, taskRunData.transaction, taskRunData)

  const stagingExceptionsExistForTaskRun =
    await executePreparedStatementInTransaction(doStagingExceptionsExistForTaskRun, context, taskRunData.transaction, taskRunData)

  const timeseriesStagingExceptionsExistForTaskRun =
    await executePreparedStatementInTransaction(doTimeseriesStagingExceptionsExistForTaskRun, context, taskRunData.transaction, taskRunData)

  if (stagingExceptionsExistForTaskRun || timeseriesStagingExceptionsExistForTaskRun) {
    context.log(`Ignoring message for task run ${taskRunData.taskRunId} - Replay of failures is not supported yet`)
    ignoreMessage = true
  } else if (timeseriesHeaderExistsForTaskRun) {
    context.log(`Ignoring message for task run ${taskRunData.taskRunId} - Timeseries header has been created and message replay is not supported yet`)
    ignoreMessage = true
  }
  return ignoreMessage
}

async function doTimeseriesStagingExceptionsExistForTaskRun (context, preparedStatement, taskRunData) {
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

async function doesTimeseriesHeaderExistForTaskRun (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)

  await preparedStatement.prepare(`
      select
        id
      from
        fff_staging.timeseries_header
      where
        task_run_id = @taskRunId     
    `)

  const parameters = {
    taskRunId: taskRunData.taskRunId
  }

  const result = await preparedStatement.execute(parameters)

  return !!(result.recordset && result.recordset[0])
}
