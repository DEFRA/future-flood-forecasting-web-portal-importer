const moment = require('moment')
const getLocationsToImportForTaskRunPlot = require('../../Shared/timeseries-functions/get-locations-to-import-for-task-run-plot')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const { getEnvironmentVariableAsAbsoluteInteger, getOffsetAsAbsoluteInteger } = require('../../Shared/utils')
const getFewsTimeParameter = require('./get-fews-time-parameter')
const getCustomOffsets = require('./get-workflow-offset-data')

module.exports = async function (context, taskRunData) {
  if (taskRunData.approved) {
    await buildPiServerUrlIfPossible(context, taskRunData)
  } else {
    context.log.warn(`Ignoring message for plot ${taskRunData.plotId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - forecast is not approved`)
  }
}

async function buildTimeParameters (context, taskRunData) {
  await buildStartAndEndTimes(context, taskRunData)
  await buildFewsTimeParameters(context, taskRunData)
}

async function buildStartAndEndTimes (context, taskRunData) {
  // Check if the workflow includes non-display group filters, if so inherit the ndg offset values
  if (taskRunData.spanWorkflow && taskRunData.spanWorkflow === true) {
    // check if there is a custom offset specified for the non-display group workflow, if not inherit the default offset
    await executePreparedStatementInTransaction(getCustomOffsets, context, taskRunData.transaction, taskRunData)
    let startTimeOffsetHours
    let endTimeOffsetHours
    if (taskRunData.offsetData.startTimeOffset && taskRunData.offsetData.startTimeOffset !== 0) {
      startTimeOffsetHours = getOffsetAsAbsoluteInteger(taskRunData.offsetData.startTimeOffset, taskRunData)
    } else {
      startTimeOffsetHours = getEnvironmentVariableAsAbsoluteInteger('FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS') || 24
    }
    if (taskRunData.offsetData.endTimeOffset && taskRunData.offsetData.endTimeOffset !== 0) {
      endTimeOffsetHours = getOffsetAsAbsoluteInteger(taskRunData.offsetData.endTimeOffset, taskRunData)
    } else {
      endTimeOffsetHours = 0 // the non display group default
    }
    taskRunData.startTime = moment(taskRunData.taskRunCompletionTime).subtract(startTimeOffsetHours, 'hours').toISOString()
    taskRunData.endTime = moment(taskRunData.taskRunCompletionTime).add(endTimeOffsetHours, 'hours').toISOString()
  } else {
    const startTimeOffsetHours = getEnvironmentVariableAsAbsoluteInteger('FEWS_DISPLAY_GROUP_START_TIME_OFFSET_HOURS') || 14
    const endTimeOffsetHours = getEnvironmentVariableAsAbsoluteInteger('FEWS_DISPLAY_GROUP_END_TIME_OFFSET_HOURS') || 120
    taskRunData.startTime = moment(taskRunData.taskRunCompletionTime).subtract(startTimeOffsetHours, 'hours').toISOString()
    taskRunData.endTime = moment(taskRunData.taskRunCompletionTime).add(endTimeOffsetHours, 'hours').toISOString()
  }
}

async function buildFewsTimeParameters (context, taskRunData) {
  taskRunData.fewsStartTime = await getFewsTimeParameter(context, taskRunData.startTime, 'startTime')
  taskRunData.fewsEndTime = await getFewsTimeParameter(context, taskRunData.endTime, 'endTime')
}

async function buildPiServerUrlIfPossible (context, taskRunData) {
  await getLocationsToImportForTaskRunPlot(context, taskRunData)

  if (taskRunData.locationIds) {
    const buildPiServerUrlCall = taskRunData.buildPiServerUrlCalls[taskRunData.piServerUrlCallsIndex]
    await buildTimeParameters(context, taskRunData)
    const plotId = `&plotId=${taskRunData.plotId}`
    const locationIds = `&locationIds=${taskRunData.locationIds.replace(/;/g, '&locationIds=')}`
    buildPiServerUrlCall.fewsParameters = `${plotId}${locationIds}${taskRunData.fewsStartTime}${taskRunData.fewsEndTime}`
    // Construct the URL used to retrieve timeseries display groups for the configured plot, locations and date range.
    buildPiServerUrlCall.fewsPiUrl =
      encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries/displaygroups?useDisplayUnits=false
        &showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON${buildPiServerUrlCall.fewsParameters}`)
  } else {
    context.log(`Ignoring message for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})` +
      `- Timeseries for all locations have been imported`)
  }
}
