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
import { getDuration } from '../../Shared/utils.js'
import getPiServerErrorMessage from '../../Shared/timeseries-functions/get-pi-server-error-message.js'
import createStagingException from '../../Shared/timeseries-functions/create-staging-exception.js'
import doIfMaximumDelayForPiServerIndexingIsNotExceeded
  from '../../Shared/timeseries-functions/do-if-maximum-delay-for-pi-server-indexing-is-not-exceeded.js'
import PartialFewsDataError from '../../Shared/message-replay/partial-fews-data-error.js'
import axios from 'axios'
import moment from 'moment'

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
  }
}

const MESSAGE_REPLAY_DELAY_MILLIS =
  getDuration(durationTypeConfig[PAUSE_BEFORE_REPLAYING_INCOMING_MESSAGE_KEY])

const OUTGOING_FILTER_MESSAGE_DELAY_MILLIS =
  getDuration(durationTypeConfig[OUTGOING_FILTER_MESSAGE_DELAY_KEY])

const OUTGOING_PLOT_MESSAGE_DELAY_MILLIS =
  getDuration(durationTypeConfig[OUTGOING_PLOT_MESSAGE_DELAY_KEY])

export default async function (context, taskRunData) {
  checkOutgoingMessages(context, taskRunData)
  const fewsResponse = await checkIfPiServerIsOnline(context, taskRunData)
  const noActionTakenMessage =
    `Outgoing messages for task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) are being output without scheduling`
  await doIfMaximumDelayForPiServerIndexingIsNotExceeded(
    { fn: checkIfAllDataForTaskRunIsAvailableAndScheduleOutgoingMessages, context, taskRunData, noActionTakenMessage }, fewsResponse
  )
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

async function checkIfAllDataForTaskRunIsAvailableAndScheduleOutgoingMessages (context, taskRunData, fewsResponse) {
  await checkIfAllDataForTaskRunIsAvailable(context, taskRunData, fewsResponse)
  await scheduleOutgoingMessages(context, taskRunData)
}

async function checkIfAllDataForTaskRunIsAvailable (context, taskRunData, fewsResponse) {
  // If the PI Server indicates that a partial response has been returned, this
  // should mean that PI Server indexing has not completed. Use defensive programming to check
  // for the Content-Range HTTP response header included with standard use of a HTTP 206 response.
  // If the header is not present, prepare to replay the message.
  //
  // If the Content-Range HTTP response header is present, this is unexpected (and should never
  // happen because PI Server requests never include a Range HTTP request header). In this case
  // create a STAGING_EXCEPTION record.
  context.log(`Checking PI Server data availability for task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  if (fewsResponse.status === 206) {
    await checkResponseHeaders(context, taskRunData, fewsResponse)
    await taskRunData.transaction.rollback()
    const config = {
      context,
      messageToReplay: taskRunData.message,
      replayDelayMillis: MESSAGE_REPLAY_DELAY_MILLIS,
      bindingName: 'processFewsEventCode'
    }
    throw new PartialFewsDataError(
      config,
      `All data is not available for task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - preparing to schedule message replay`
    )
  }
}

async function checkResponseHeaders (context, taskRunData, fewsResponse) {
  if (fewsResponse?.headers?.['Content-Range']) {
    const errorText = 'Received unexpected Content-Range header when checking PI Server data availability for task run'
    taskRunData.errorMessage = `${errorText} ${taskRunData.taskRunId}`
    await createStagingException(context, taskRunData)
  }
}

function checkOutgoingMessages (context, taskRunData) {
  const filterMessages = taskRunData.outgoingMessages.filter(message => message.filterId)
  const plotMessages = taskRunData.outgoingMessages.filter(message => message.plotId)
  taskRunData.filterMessageCreated = filterMessages.length > 0
  taskRunData.plotMessageCreated = plotMessages.length > 0
}

function scheduleOutgoingMessages (context, taskRunData) {
  // The task run needs data retrieving for one or more plots and/or a PI Server instance has
  // confirmed it can provide all filter based data for the task run.
  // As a PI Server cannot indicate if all data for a task run involving one or more plots
  // is available, more time could be required for PI Server indexing to complete.
  // Similarly, if multiple PI Servers instances are available, more time could be
  // required for PI Server indexing to complete on ALL available instances before
  // data retrieval is attempted. Outgoing messages need to be scheduled accordingly.
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
}
