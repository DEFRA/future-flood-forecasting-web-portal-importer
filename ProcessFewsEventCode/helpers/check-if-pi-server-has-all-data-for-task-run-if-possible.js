// This module exists in response to INC1338365 and INC2182094.
// It is possible for core forecasting engine task run messages to be received before
// associated PI Server indexing has completed for the task run. If PI Server
// data retrieval for a task run is performed before indexing is completed, incomplete
// data can be retrieved resulting in data gaps.
//
// Ideally, task run completion messages should not be sent until PI Server indexing
// is complete. Until such functionality is implemented, client interaction with the PI
// Server needs to take account of the fact that PI Server indexing might not have
// completed when a task run completion message is received. This results in client
// code attempting to delay subsequent processing until PI Server indexing has completed
// to minimise the risk of data gaps.
const { getEnvironmentVariableAsAbsoluteInteger } = require('../../Shared/utils')
const getPiServerErrorMessage = require('../../Shared/timeseries-functions/get-pi-server-error-message')
const createStagingException = require('../../Shared/timeseries-functions/create-staging-exception')
const axios = require('axios')
const azureServiceBus = require('@azure/service-bus')
const moment = require('moment')

const MAXIMUM_DELAY_FOR_PI_SERVER_DATA_AVAILABILITY_AFTER_TASK_RUN_COMPLETION_KEY = 'maximumDelayForPiServerDataAvailabilityAfterTaskRunCompletion'
const PAUSE_BEFORE_REPLAYING_INCOMING_MESSAGE_KEY = 'pauseBeforeReplayingIncomingMessage'
const OUTGOING_FILTER_MESSAGE_DELAY_KEY = 'outgoingFilterMessageDelay'
const OUTGOING_PLOT_MESSAGE_DELAY_KEY = 'outgoingPlotMessageDelay'

const durationTypeConfig = {
  [PAUSE_BEFORE_REPLAYING_INCOMING_MESSAGE_KEY]: {
    environmentVariableName: 'CHECK_FOR_TASK_RUN_DATA_AVAILABILITY_DELAY_MILLIS',
    defaultDuration: 2000
  },
  [OUTGOING_FILTER_MESSAGE_DELAY_KEY]: {
    environmentVariableName: 'WAIT_FOR_TASK_RUN_FILTER_DATA_AVAILABILITY_MILLIS',
    defaultDuration: 5000
  },
  [OUTGOING_PLOT_MESSAGE_DELAY_KEY]: {
    environmentVariableName: 'WAIT_FOR_TASK_RUN_PLOT_DATA_AVAILABILITY_MILLIS',
    defaultDuration: 15000
  },
  [MAXIMUM_DELAY_FOR_PI_SERVER_DATA_AVAILABILITY_AFTER_TASK_RUN_COMPLETION_KEY]: {
    environmentVariableName: 'MAXIMUM_DELAY_FOR_DATA_AVAILABILITY_AFTER_TASK_RUN_COMPLETION_MILLIS',
    defaultDuration: 120000
  }
}

const MAXIMUM_NUMBER_OF_MILLISECONDS_AFTER_TASK_RUN_COMPLETION_TO_ALLOW_FOR_PI_SERVER_DATA_AVAILABILITY =
  getDuration(durationTypeConfig[MAXIMUM_DELAY_FOR_PI_SERVER_DATA_AVAILABILITY_AFTER_TASK_RUN_COMPLETION_KEY])

const OUTGOING_FILTER_MESSAGE_DELAY_MILLIS =
  getDuration(durationTypeConfig[OUTGOING_FILTER_MESSAGE_DELAY_KEY])

const OUTGOING_PLOT_MESSAGE_DELAY_MILLIS =
  getDuration(durationTypeConfig[OUTGOING_PLOT_MESSAGE_DELAY_KEY])

const TASK_RUN_COMPLETION_MESSAGE_FRAGMENT =
 `the task run completed more than ${MAXIMUM_NUMBER_OF_MILLISECONDS_AFTER_TASK_RUN_COMPLETION_TO_ALLOW_FOR_PI_SERVER_DATA_AVAILABILITY / 1000} second(s) ago`

// Use lazy instantiation for an insance of ServiceBusAdministrationClient to allow mocking.
let serviceBusAdministrationClient

module.exports = async function (context, taskRunData) {
  checkOutgoingMessages(context, taskRunData)
  const fewsResponse = await checkIfPiServerIsOnline(context, taskRunData)
  if (taskRunData.filterMessageCreated) {
    await checkIfAllDataForTaskRunIsAvailable(context, taskRunData, fewsResponse)
  }

  // The task run needs data retrieving for one or more plots and/or a PI Server instance has
  // confirmed it can provide all filter based data for the task run.
  // As a PI Server cannot indicate if all data for a task run involving one or more plots
  // is available, more time could be required for PI Server indexing to complete.
  // Similarly, if multiple PI Servers instances are available, more time could be
  // required for PI Server indexing to complete on ALL available instances before
  // data retrieval is attempted. Outgoing messages might need to be scheduled
  // accordingly.
  scheduleOutgoingMessagesIfNeeded(context, taskRunData)
}

async function checkIfPiServerIsOnline (context, taskRunData) {
  // Chek if the PI Server is online by calling a specific PI Server endpoint
  // dependent on whether plot and/or filter data needs to be retrieved for the task run.
  // The PI Server endpoint to be called and any associated endpoint specific error messaging
  // is calculated by the getFragments function.
  const fewsPiUrlRoot = `${process.env.FEWS_PI_API}/FewsWebServices/rest/fewspiservice/v1/`
  const { errorMessageFragment, fewsPiUrlFragment } = getFragments(context, taskRunData)
  const messageFragment = `for task run ${taskRunData.taskRunId} of workflow ${taskRunData.workflowId}`
  try {
    const fewsPiUrl = encodeURI(`${fewsPiUrlRoot}${fewsPiUrlFragment}documentFormat=PI_JSON`)
    context.log(`Checking if PI Server is online ${messageFragment}`)
    // INC2182094 - Use a HTTP HEAD request in preference to a HTTP GET request because
    // response data is not needed to determine if the PI Server is online. Additionally,
    // if filter data needs to be retrieved for the task run, only the HTTP response code is
    // required to determine if all data for the task run is avaliable.
    const fewsResponse = await axios.head(fewsPiUrl)
    context.log(`PI Server is online ${messageFragment}`)
    return fewsResponse
  } catch (err) {
    if (typeof err.response === 'undefined') {
      context.log.error(`PI Server is unavailable ${messageFragment}`)
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
    // If data needs to be retrieved using one or more filters, prepare to check
    // if all data for the task run is available from the PI Server before sending outgoing
    // messages.
    // INC2182094 - The archive server is queried by default when asking if all data for
    // the task run is available. As response times can be slow when querying the archive
    // server using a task run ID, use the importFromExternalDataSource parameter to prevent
    // the archive server from being queried.
    fewsPiUrlFragment = `timeseries?taskRunIds=${taskRunData.taskRunId}&importFromExternalDataSource=false&`
    errorMessageFragment = `all data for task run ${taskRunData.taskRunId} is available`
  } else {
    // If data needs to be retrieved using one or more plots, the PI Server
    // cannot indicate if all data for the task run is available yet, so just prepare
    // to check if the PI Server is online.
    fewsPiUrlFragment = 'filters?'
    errorMessageFragment = 'PI Server is available'
  }
  return { errorMessageFragment, fewsPiUrlFragment }
}

async function checkIfAllDataForTaskRunIsAvailable (context, taskRunData, fewsResponse) {
  // If the PI Server indicates that a partial response has been returned, this
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
  if (!serviceBusAdministrationClient) {
    serviceBusAdministrationClient =
      new azureServiceBus.ServiceBusAdministrationClient(process.env.AzureWebJobsServiceBus)
  }

  const fewsEventCodeQueue =
    await serviceBusAdministrationClient.getQueue('fews-eventcode-queue')

  const warningMessage = `All data is not available for task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`

  if (context.bindingData.deliveryCount < (fewsEventCodeQueue.maxDeliveryCount - 1)) {
    // The message delivery count (zero based) is less than the maximum delivery count
    // so pause before replaying the message.
    await sleep(durationTypeConfig[PAUSE_BEFORE_REPLAYING_INCOMING_MESSAGE_KEY])
    throw new Error(warningMessage)
  } else {
    // This is the final attempt at replaying the message and all data for the filter based
    // task run is not available. Allow message processing to continue so that available
    // data can be loaded.
    context.log.warn(`${warningMessage} and maximum number of replay attempts has been reached. Loading available data rather than no data`)
  }
}

async function checkResponseHeaders (context, taskRunData, fewsResponse) {
  if (fewsResponse?.headers?.['Content-Range']) {
    const errorText = 'Received unexpected Content-Range header when checking PI Server data availability for task run'
    taskRunData.errorMessage = `${errorText} ${taskRunData.taskRunId}`
    await createStagingException(context, taskRunData)
  }
}

function getDuration (durationType) {
  return getEnvironmentVariableAsAbsoluteInteger(durationType.environmentVariableName) || durationType.defaultDuration
}

async function sleep (sleepType) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve()
    }, getDuration(sleepType))
  })
}

function checkOutgoingMessages (context, taskRunData) {
  const filterMessages = taskRunData.outgoingMessages.filter(message => message.filterId)
  const plotMessages = taskRunData.outgoingMessages.filter(message => message.plotId)
  taskRunData.filterMessageCreated = filterMessages.length > 0
  taskRunData.plotMessageCreated = plotMessages.length > 0
}

function scheduleOutgoingMessagesIfNeeded (context, taskRunData) {
  const millisecondsSinceTaskRunCompletion = moment.utc().diff(moment.utc(new Date(`${taskRunData.taskRunCompletionTime}`)), 'milliseconds')

  // Outgoing messages need to be scheduled if a reasonable amount of tume for PI Server indexing to complete on all available
  // instances has not passed since task run completion.
  if (millisecondsSinceTaskRunCompletion < MAXIMUM_NUMBER_OF_MILLISECONDS_AFTER_TASK_RUN_COMPLETION_TO_ALLOW_FOR_PI_SERVER_DATA_AVAILABILITY) {
    context.log(`Scheduling outgoing message(s) to allow PI Server indexing to complete for task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
    // Schedule outgoing filter based messages to minimise the risk of data retrieval being attempted using an available PI Server
    // instance for which indexing has not completed. As indexing has completed on at least one available PI Server instance, it
    // should not take too long for indexing to complete on all available instances.
    const filterScheduledEnqueueTimeUtc = moment.utc().add(OUTGOING_FILTER_MESSAGE_DELAY_MILLIS, 'milliseconds').toDate()
    // A PI Server instance cannot indicate if it can provide all plot based data for a task run so prepare to schedule outgoing
    // plot based messages so that indexing has more time to complete.
    const plotScheduledEnqueueTimeUtc = moment.utc().add(OUTGOING_PLOT_MESSAGE_DELAY_MILLIS, 'milliseconds').toDate()
    taskRunData.outgoingMessages = taskRunData.outgoingMessages.map(outgoingMessage => {
      const scheduledEnqueueTimeUtc =
        outgoingMessage.filterId ? filterScheduledEnqueueTimeUtc : plotScheduledEnqueueTimeUtc

      return {
        body: outgoingMessage,
        scheduledEnqueueTimeUtc
      }
    })
  } else {
    const noSchedulingMessageFragment =
      `Outgoing messages for task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) are being output without scheduling`
    context.log(`${noSchedulingMessageFragment} because ${TASK_RUN_COMPLETION_MESSAGE_FRAGMENT}`)
  }
}
