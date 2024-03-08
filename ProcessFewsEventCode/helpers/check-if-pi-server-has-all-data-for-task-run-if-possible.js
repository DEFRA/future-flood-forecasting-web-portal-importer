const { getEnvironmentVariableAsAbsoluteInteger } = require('../../Shared/utils')
const getPiServerErrorMessage = require('../../Shared/timeseries-functions/get-pi-server-error-message')
const createStagingException = require('../../Shared/timeseries-functions/create-staging-exception')
const axios = require('axios')
const azureServiceBus = require('@azure/service-bus')

const PAUSE_BEFORE_REPLAYING_INCOMING_MESSAGE = 'pauseBeforeReplayingIncomingMessage'
const PAUSE_BEFORE_SENDING_OUTGOING_MESSAGES = 'pauseBeforeSendingOutgoingMessages'

const sleepTypeConfig = {
  [PAUSE_BEFORE_REPLAYING_INCOMING_MESSAGE]: {
    environmentVariableName: 'CHECK_FOR_TASK_RUN_DATA_AVAILABILITY_DELAY_MILLIS',
    defaultDuration: 2000
  },
  [PAUSE_BEFORE_SENDING_OUTGOING_MESSAGES]: {
    environmentVariableName: 'WAIT_FOR_TASK_RUN_DATA_AVAILABILITY_MILLIS',
    defaultDuration: 15000
  }
}

module.exports = async function (context, taskRunData) {
  checkOutgoingMessages(context, taskRunData)
  const fewsResponse = await checkIfPiServerIsOnline(context, taskRunData)
  if (taskRunData.filterMessageCreated) {
    await checkIfAllDataForTaskRunIsAvailable(context, taskRunData, fewsResponse)
  }
  if (taskRunData.plotMessageCreated) {
    await pauseBeforeSendingOutgoingMessagesIfNeeded(context, taskRunData)
  }
}

async function pauseBeforeSendingOutgoingMessagesIfNeeded (context, taskRunData) {
  // INC1338365 - The PI Server is online but cannot indicate if all data for a task run
  // involving one or more plots is available. If no TIMESERIES_HEADER record existed for the
  // task run at the start of the current message processing attempt, try and prevent incomplete
  // data from being returned from the PI Server by pausing to allow PI Server indexing to
  // complete before sending a message for each plot for which data is to be retrieved.
  // A pause for PI Server indexing to complete has happened already if a TIMESERIES_HEADER
  // record existed for the task run at the start of the current message processing attempt,
  //
  // This should prevent incomplete data retrieval in most cases and is a workaround
  // until a more robust long term solution can be implemented.
  if (!taskRunData.timeseriesHeaderExistsForTaskRun) {
    await sleep(sleepTypeConfig[PAUSE_BEFORE_SENDING_OUTGOING_MESSAGES])
  }
}

async function checkIfPiServerIsOnline (context, taskRunData) {
  const fewsPiUrlRoot = `${process.env.FEWS_PI_API}/FewsWebServices/rest/fewspiservice/v1/`
  const { errorMessageFragment, fewsPiUrlFragment } = getFragments(context, taskRunData)

  try {
    const fewsPiUrl = encodeURI(`${fewsPiUrlRoot}${fewsPiUrlFragment}documentFormat=PI_JSON`)
    const fewsResponse = await axios.get(fewsPiUrl)
    return fewsResponse
  } catch (err) {
    if (typeof err.response === 'undefined') {
      context.log.error('PI Server is unavailable')
    } else {
      const piServerErrorMessage = getPiServerErrorMessage(context, err)
      context.log.error(`An unexpected error occurred when checking if ${errorMessageFragment} - ${err.message} (${piServerErrorMessage})`)
    }
    // Attempt message replay.
    throw err
  }
}

function getFragments (context, taskRunData) {
  let errorMessageFragment
  let fewsPiUrlFragment

  if (taskRunData.filterMessageCreated) {
    // INC1338365 - If data needs to be retrieved using one or more filters prepare to check
    // if all data for the task run is available from the PI Server before sending outgoing
    // messages.
    fewsPiUrlFragment = `timeseries?taskRunIds=${taskRunData.taskRunId}&onlyHeaders=true&`
    errorMessageFragment = `all data for ${taskRunData.taskRunId} is available`
  } else {
    // INC1338365 - If data needs to be retrieved using one or more plots, the PI Server
    // cannot indicate if all data for the task run is available yet, so just prepare
    // to check if the PI Server is online.
    fewsPiUrlFragment = 'filters?'
    errorMessageFragment = 'PI Server is available'
  }
  return { errorMessageFragment, fewsPiUrlFragment }
}

async function checkIfAllDataForTaskRunIsAvailable (context, taskRunData, fewsResponse) {
  // INC1338365 - If the PI Server indicates that a partial response has been returned, this
  // should mean that PI Server indexing has not completed. Use defensive programming to check
  // for the Content-Range HTTP response header included with standard use of a HTTP 206 response.
  // If the header is not present, pause for a configurable amount of time (to try and prevent
  // the PI Server being overloaded) and then send the message for replay.
  //
  // If the Content-Range HTTP response header is present, this is unexpected (and should never
  // happen because PI Server requests never include a Range HTTP request header). In this case
  // create a STAGING_EXCEPTION record.
  context.log(`Checking PI Server data availability for task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  if (fewsResponse.status === 206) {
    await checkResponseHeaders(context, taskRunData, fewsResponse)
    await replayMessageIfNeeded(context, taskRunData)
  }
}

async function replayMessageIfNeeded (context, taskRunData) {
  const serviceBusAdministrationClient =
    new azureServiceBus.ServiceBusAdministrationClient(process.env.AzureWebJobsServiceBus)

  const fewsEventCodeQueue =
    await serviceBusAdministrationClient.getQueue('fews-eventcode-queue')

  if (context.bindingData.deliveryCount < (fewsEventCodeQueue.maxDeliveryCount - 1)) {
    // INC1338365 - The message delivery count (zero based) is less than the maximum delivery count,
    // so pause before replaying the message.
    await sleep(sleepTypeConfig[PAUSE_BEFORE_REPLAYING_INCOMING_MESSAGE])
    throw new Error(`All data is not available for task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  } else {
    // INC1338365 -This is the final attempt at replaying the message and all data for the filter based
    // task run is not available. Allow message processing to continue so that available
    // data can be loaded.
  }
}

async function checkResponseHeaders (context, taskRunData, fewsResponse) {
  if (fewsResponse?.headers?.['Content-Range']) {
    const errorText = 'Received unexpected Content-Range header when checking PI Server data availability for task run'
    taskRunData.errorMessage = `${errorText} ${taskRunData.taskRunId}`
    await createStagingException(context, taskRunData)
  }
}

async function sleep (sleepType) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve()
    }, getEnvironmentVariableAsAbsoluteInteger(sleepType.environmentVariableName) || sleepType.defaultDuration)
  })
}

function checkOutgoingMessages (context, taskRunData) {
  const filterMessages = taskRunData.outgoingMessages.filter(message => message.filterId)
  const plotMessages = taskRunData.outgoingMessages.filter(message => message.plotId)
  taskRunData.filterMessageCreated = filterMessages.length > 0
  taskRunData.plotMessageCreated = plotMessages.length > 0
}
