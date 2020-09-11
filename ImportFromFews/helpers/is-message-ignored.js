const doTimeseriesExistForTaskRunPlotOrFilter = require('./do-timeseries-exist-for-task-run-plot-or-filter')
const doActiveTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter = require('./do-active-timeseries-staging-exceptions-exist-for-task-run-plot-or-filter')
const isIgnoredWorkflow = require('../../Shared/timeseries-functions/is-ignored-workflow')

module.exports = async function (context, taskRunData) {
  let ignoreMessage = false
  if (await isIgnoredWorkflow(context, taskRunData)) {
    context.log(`${taskRunData.workflowId} is an ignored workflow`)
  } else {
    const timeseriesExistForTaskRunPlotOrFilter =
      await doTimeseriesExistForTaskRunPlotOrFilter(context, taskRunData)

    const timeseriesStagingExceptionsExistForTaskRunPlotOrFilter =
      await doActiveTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter(context, taskRunData)

    if (timeseriesExistForTaskRunPlotOrFilter && !timeseriesStagingExceptionsExistForTaskRunPlotOrFilter) {
      context.log(`Ignoring message for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - Timeseries have been imported`)
      ignoreMessage = true
    }
  }
  return ignoreMessage
}
