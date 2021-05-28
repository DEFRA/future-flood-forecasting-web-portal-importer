const isLatestTaskRunForWorkflow = require('../../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const isNonDisplayGroupForecast = require('./is-non-display-group-forecast')
const { logObsoleteTaskRunMessage } = require('../../Shared/utils')

module.exports = async function (context, taskRunData, checkForNonDisplayGroupForecast, processingFunction) {
  if (await isNonForecastOrLatestTaskRunForWorkflow(context, taskRunData, checkForNonDisplayGroupForecast)) {
    await processingFunction(context, taskRunData)
  } else {
    logObsoleteTaskRunMessage(context, taskRunData)
  }
}

async function isNonForecastOrLatestTaskRunForWorkflow (context, taskRunData, checkForNonDisplayGroupForecast) {
  const isForecast = checkForNonDisplayGroupForecast ? isNonDisplayGroupForecast(context, taskRunData) : taskRunData.forecast
  return !isForecast || await isLatestTaskRunForWorkflow(context, taskRunData)
}
