const doTimeseriesExistForTaskRunPlotOrFilter = require('../../Shared/timeseries-functions/do-timeseries-exist-for-task-run-plot-or-filter')
const doActiveTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter = require('./do-active-timeseries-staging-exceptions-exist-for-task-run-plot-or-filter')
const isIgnoredWorkflow = require('../../Shared/timeseries-functions/is-ignored-workflow')

module.exports = async function (context, taskRunData) {
  let ignoreMessage = false
  if (await isIgnoredWorkflow(context, taskRunData)) {
    context.log(`${taskRunData.workflowId} is an ignored workflow`)
  } else if (taskRunData.filterId) {
    // Ignore filter based messages with associated timeseries and no associated timeseries staging exceptions.
    // It is not possible to ignore plot based messages in this way as it is possible for subsets of plot locations to be
    // loaded if CSV files contain configuration errors.
    // For example, consider the case where the coastal or fluvial display group CSV file contains a plot ID typo for
    // a subset of plot locations. If the typo is corrected the core engine message has to be replayed as it is not
    // possible to know which plot message(s) for the core engine task run should be replayed. Replay of the core engine message will cause
    // deactivation of timeseries staging exceptions associated with corrected plot ID typos (as the erroneous plot IDs no longer exist
    // - see ProcessFewsEventCode/helpers/deactivate-timeseries-staging-exceptions-for-non-existent-task-run-plots-and-filters).
    // If the criteria for ignoring filter based messages was applied to plot based messages, in the scenario described above missing subsets
    // of locations would not load following core engine message replay after CSV correction. As such, plot based messages need to be replayed.
    // Additional code will ignore the message if timeseries exist for all plot locations.
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
