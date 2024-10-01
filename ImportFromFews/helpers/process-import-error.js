import createTimeseriesStagingException from './create-timeseries-staging-exception.js'
import getPiServerErrorMessage from '../../Shared/timeseries-functions/get-pi-server-error-message.js'
import PartialFewsDataError from '../../Shared/message-replay/partial-fews-data-error.js'
import TimeseriesStagingError from '../../Shared/timeseries-functions/timeseries-staging-error.js'

export default async function (context, taskRunData, err) {
  if (err instanceof PartialFewsDataError) {
    throw err
  } else if (!(err instanceof TimeseriesStagingError) && typeof err.response === 'undefined') {
    context.log.error(`Failed to connect to ${process.env.FEWS_PI_API}`)
    // If connection to the PI Server fails propagate the failure so that standard Azure message replay
    // functionality is used.
    throw err
  } else {
    const errorData = await prepareTimeseriesStagingExceptionData(context, taskRunData, err)
    await createTimeseriesStagingException(context, errorData)
  }
}

async function prepareTimeseriesStagingExceptionData (context, taskRunData, err) {
  let errorData
  if (err instanceof TimeseriesStagingError) {
    errorData = err.context
  } else {
    errorData = await createTimeseriesStagingExceptionData(context, taskRunData, err)
  }
  return errorData
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
    fewsParameters: taskRunData.fewsParameters || null,
    payload: taskRunData.message,
    timeseriesHeaderId: taskRunData.timeseriesHeaderId,
    description: errorDescription,
    csvError,
    csvType
  }
}
