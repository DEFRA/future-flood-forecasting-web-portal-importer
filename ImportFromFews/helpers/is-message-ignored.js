const doTimeseriesExistForTaskRunPlotOrFilter = require('./do-timeseries-exist-for-task-run-plot-or-filter')
const doTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter = require('./do-timeseries-staging-exceptions-exist-for-task-run-plot-or-filter')
const isIgnoredWorkflow = require('../../Shared/timeseries-functions/is-ignored-workflow')

module.exports = async function (context, taskRunData) {
  let ignoreMessage = false
  if (await isIgnoredWorkflow(context, taskRunData)) {
    context.log(`${taskRunData.workflowId} is an ignored workflow`)
  } else {
    const timeseriesExistForTaskRunPlotOrFilter =
      await doTimeseriesExistForTaskRunPlotOrFilter(context, taskRunData)

    const timeseriesStagingExceptionsExistForTaskRunPlotOrFilter =
      await doTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter(context, taskRunData)

    if (timeseriesStagingExceptionsExistForTaskRunPlotOrFilter) {
      context.log(`Ignoring message for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - Replay of failures is not supported yet`)
      ignoreMessage = true
    } else if (timeseriesExistForTaskRunPlotOrFilter) {
      context.log(`Ignoring message for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - Timeseries have been imported`)
      ignoreMessage = true
    }
  }
  return ignoreMessage
}
