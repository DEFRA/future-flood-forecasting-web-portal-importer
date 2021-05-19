const isLatestTaskRunForWorkflow = require('../../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const isNonDisplayGroupForecast = require('./is-non-display-group-forecast')

module.exports = async function (context, taskRunData, checkForNonDisplayGroupForecast) {
  const isForecast = checkForNonDisplayGroupForecast ? isNonDisplayGroupForecast(context, taskRunData) : taskRunData.forecast
  return !isForecast || await isLatestTaskRunForWorkflow(context, taskRunData)
}
