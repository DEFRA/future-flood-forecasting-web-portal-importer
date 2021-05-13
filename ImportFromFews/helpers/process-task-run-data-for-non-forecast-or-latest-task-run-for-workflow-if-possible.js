import isLatestTaskRunForWorkflow from '../../Shared/timeseries-functions/is-latest-task-run-for-workflow.js'
import isNonDisplayGroupForecast from './is-non-display-group-forecast.js'
import { logObsoleteTaskRunMessage } from '../../Shared/utils.js'

export default async function (context, taskRunData, checkForNonDisplayGroupForecast, processingFunction) {
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
