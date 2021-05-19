const createTimeseriesStagingException = require('./create-timeseries-staging-exception')
const getPiServerErrorMessage = require('../../Shared/timeseries-functions/get-pi-server-error-message')
const TimeseriesStagingError = require('../../Shared/timeseries-functions/timeseries-staging-error')

module.exports = async function (context, taskRunData, err) {
  let errorData
  if (!(err instanceof TimeseriesStagingError) && typeof err.response === 'undefined') {
    context.log.error(`Failed to connect to ${process.env.FEWS_PI_API}`)
    // If connection to the PI Server fails propagate the failure so that standard Azure message replay
    // functionality is used.
    throw err
  } else {
    // For other errors create a timeseries staging exception to indicate that
    // manual intervention is required before replay of the task run is attempted.
    if (err instanceof TimeseriesStagingError) {
      errorData = err.context
    } else {
      errorData = await createTimeseriesStagingExceptionData(context, taskRunData, err)
    }
    await createTimeseriesStagingException(context, errorData)
  }
}

async function createTimeseriesStagingExceptionData (context, taskRunData, err) {
  const csvError = (err.response && err.response.status === 400) || false
  const csvType = csvError ? taskRunData.csvType : null
  const piServerErrorMessage = await getPiServerErrorMessage(context, err)
  const errorDescription = `An error occurred while processing data for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}): ${piServerErrorMessage}`
  return {
    transaction: taskRunData.transaction,
    sourceId: taskRunData.sourceId,
    sourceType: taskRunData.sourceType,
    csvError: csvError,
    csvType: csvType,
    fewsParameters: taskRunData.fewsParameters || null,
    payload: taskRunData.message,
    timeseriesHeaderId: taskRunData.timeseriesHeaderId,
    description: errorDescription
  }
}
