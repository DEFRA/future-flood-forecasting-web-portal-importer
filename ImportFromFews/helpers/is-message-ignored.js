const doStagingExceptionsExistForTaskRun = require('../../Shared/timeseries-functions/do-staging-exceptions-exist-for-task-run')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const isIgnoredWorkflow = require('../../Shared/timeseries-functions/is-ignored-workflow')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  let ignoreMessage = false
  if (await executePreparedStatementInTransaction(isIgnoredWorkflow, context, taskRunData.transaction, taskRunData.workflowId)) {
    context.log(`${taskRunData.workflowId} is an ignored workflow`)
  } else {
    const stagingExceptionsExistForTaskRun =
      await executePreparedStatementInTransaction(doStagingExceptionsExistForTaskRun, context, taskRunData.transaction, taskRunData)

    const timeseriesExistForTaskRunPlotOrFilter =
      await executePreparedStatementInTransaction(doTimeseriesExistForTaskRunPlotOrFilter, context, taskRunData.transaction, taskRunData)

    const timeseriesStagingExceptionsExistForTaskRunPlotOrFilter =
      await executePreparedStatementInTransaction(doTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter, context, taskRunData.transaction, taskRunData)

    if (stagingExceptionsExistForTaskRun || timeseriesStagingExceptionsExistForTaskRunPlotOrFilter) {
      context.log(`Ignoring message for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - Replay of failures is not supported yet`)
      ignoreMessage = true
    } else if (timeseriesExistForTaskRunPlotOrFilter) {
      context.log(`Ignoring message for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - Timeseries have been imported`)
      ignoreMessage = true
    }
  }
  return ignoreMessage
}

async function doTimeseriesExistForTaskRunPlotOrFilter (context, preparedStatement, taskRunData) {
  const fewsParameters = taskRunData.plotId ? `&plotId=${taskRunData.plotId}%` : `&filterId=${taskRunData.filterId}%`
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('fewsParameters', sql.NVarChar)

  await preparedStatement.prepare(`
    select
      t.id
    from
      fff_staging.timeseries_header th,
      fff_staging.timeseries t
     where
      th.task_run_id = @taskRunId and
      th.id = t.timeseries_header_id and
      t.fews_parameters like @fewsParameters     
    `)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    fewsParameters: fewsParameters
  }

  const result = await preparedStatement.execute(parameters)

  return !!(result && result.recordset && result.recordset[0])
}

async function doTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('sourceId', sql.NVarChar)
  await preparedStatement.input('sourceType', sql.NChar)

  await preparedStatement.prepare(`
    select
      tse.id
    from
      fff_staging.timeseries_header th,
      fff_staging.timeseries_staging_exception tse
    where
      th.task_run_id = @taskRunId and
      th.id = tse.timeseries_header_id and
      tse.source_id = @sourceId and
      tse.source_type = @sourceType
    `)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    sourceId: taskRunData.sourceId,
    sourceType: taskRunData.sourceType
  }

  const result = await preparedStatement.execute(parameters)

  return !!(result && result.recordset && result.recordset[0])
}
