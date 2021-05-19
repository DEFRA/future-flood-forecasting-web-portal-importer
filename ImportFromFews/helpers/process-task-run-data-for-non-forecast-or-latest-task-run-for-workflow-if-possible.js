const isLatestTaskRunForWorkflow = require('../../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const isNonDisplayGroupForecast = require('./is-non-display-group-forecast')

module.exports = async function (context, taskRunData, checkForNonDisplayGroupForecast, processingFunction) {
  if (await isNonForecastOrLatestTaskRunForWorkflow(context, taskRunData, checkForNonDisplayGroupForecast)) {
    await processingFunction(context, taskRunData)
  } else {
    context.log.warn(`Ignoring message for ${taskRunData.sourceDetails} completed on ${taskRunData.taskRunCompletionTime}` +
    ` - ${taskRunData.latestTaskRunId} completed on ${taskRunData.latestTaskRunCompletionTime} is the latest task run for workflow ${taskRunData.workflowId}`)
  }
}

async function isNonForecastOrLatestTaskRunForWorkflow (context, taskRunData, checkForNonDisplayGroupForecast) {
  const isForecast = checkForNonDisplayGroupForecast ? isNonDisplayGroupForecast(context, taskRunData) : taskRunData.forecast
  return !isForecast || await isLatestTaskRunForWorkflow(context, taskRunData)
}
