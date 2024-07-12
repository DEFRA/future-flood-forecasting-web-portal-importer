const deactivateObsoleteStagingExceptionsBySourceFunctionAndWorkflowId = require('../Shared/timeseries-functions/deactivate-obsolete-staging-exceptions-by-source-function-and-workflow-id')
const { deactivateStagingExceptionBySourceFunctionAndTaskRunId, deactivateTimeseriesStagingExceptionsForNonExistentTaskRunPlotsAndFilters } = require('../Shared/timeseries-functions/deactivation-utils')
const getTaskRunPlotsAndFiltersToBeProcessed = require('./helpers/get-task-run-plots-and-filters-to-be-processed')
const isLatestTaskRunForWorkflow = require('../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const doesTimeseriesHeaderExistForTaskRun = require('./helpers/does-timeseries-header-exist-for-task-run')
const createStagingException = require('../Shared/timeseries-functions/create-staging-exception')
const isIgnoredWorkflow = require('../Shared/timeseries-functions/is-ignored-workflow')
const isSpanWorkflow = require('../Shared/timeseries-functions/check-spanning-workflow')
const getTaskRunCompletionDate = require('./helpers/get-task-run-completion-date')
const checkIfPiServerHasAllDataForTaskRunIfPossible = require('./helpers/check-if-pi-server-has-all-data-for-task-run-if-possible')
const StagingError = require('../Shared/timeseries-functions/staging-error')
const createTimeseriesHeader = require('./helpers/create-timeseries-header')
const getTaskRunStartDate = require('./helpers/get-task-run-start-date')
const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const isTaskRunApproved = require('./helpers/is-task-run-approved')
const preprocessMessage = require('./helpers/preprocess-message')
const getWorkflowId = require('./helpers/get-workflow-id')
const getTaskRunId = require('./helpers/get-task-run-id')
const isForecast = require('./helpers/is-forecast')
const { logObsoleteTaskRunMessage } = require('../Shared/utils')
const PartialFewsDataError = require('../Shared/message-replay/partial-fews-data-error')
const processPartialFewsDataError = require('../Shared/message-replay/process-partial-fews-data-error')
const publishScheduledMessagesIfNeeded = require('../Shared/timeseries-functions/publish-scheduled-messages-if-needed')
const moment = require('moment')

const sourceTypeLookup = {
  F: {
    description: 'filter',
    messageKey: 'filterId'
  },
  P: {
    description: 'plot',
    messageKey: 'plotId'
  }
}

module.exports = async function (context, message) {
  // This function is triggered via a queue message drop, 'message' is the name of the variable that contains the queue item payload.
  const errorMessage = 'The message routing function has failed with the following error:'
  const isolationLevel = null
  const messageToLog = typeof message === 'string' ? message : JSON.stringify(message)
  context.log(`Processing core engine message: ${messageToLog}`)
  await doInTransaction({ fn: processMessage, context, errorMessage, isolationLevel }, message)

  // If context.bindings.processFewsEventCode exists, this indicates that all data
  // is not available for the task run and the message needs to be replayed.
  // If context.bindings.processFewsEventCode does not exist, either of the following is true
  // and message processing (scheduled or not) should proceed:
  // - All data is available for the task run
  // - Data availability for the task run cannot be determined
  const scheduledMessageConfig = {
    destinationName: context.bindings.processFewsEventCode ? 'fews-eventcode-queue' : 'fews-import-queue',
    outputBinding: context.bindings.processFewsEventCode ? 'processFewsEventCode' : 'importFromFews'
  }

  // In common with messages published using context bindings, publish scheduled messages outside of the
  // transaction used during message processing.
  await publishScheduledMessagesIfNeeded(context, scheduledMessageConfig)
  // context.done() not required in async functions
}

async function buildAndProcessOutgoingWorkflowMessagesIfPossible (context, transaction, taskRunData) {
  // Process data for each CSV file associated with the workflow.
  await getTaskRunPlotsAndFiltersToBeProcessed(context, taskRunData)

  // Create a message for each plot/filter to be processed for the task run.
  // (see https://medium.com/@antonioval/making-array-iteration-easy-when-using-async-await-6315c3225838)
  await Promise.all(taskRunData.itemsToBeProcessed.map(async itemToBeProcessed => {
    await createOutgoingMessageIfPossible(context, taskRunData, itemToBeProcessed)
  }))

  await processOutgoingMessagesIfPossible(context, taskRunData)
}

async function createOutgoingMessageIfPossible (context, taskRunData, itemToBeProcessed) {
  if ((itemToBeProcessed.sourceType === 'P' || taskRunData.spanWorkflow) && taskRunData.forecast && !taskRunData.approved) {
    context.log.warn(`Ignoring data for plot ID ${itemToBeProcessed.sourceId} of unapproved forecast message ${JSON.stringify(taskRunData.message)}`)
  } else {
    const message = {
      taskRunId: taskRunData.taskRunId
    }
    message[sourceTypeLookup[itemToBeProcessed.sourceType].messageKey] = itemToBeProcessed.sourceId
    taskRunData.outgoingMessages.push(message)
    context.log(`Created message for ${sourceTypeLookup[itemToBeProcessed.sourceType].description} ID ${itemToBeProcessed.sourceId}`)
  }
}

async function parseMessageAndProcessTaskRunDataIfPossible (context, transaction, preprocessedMessage) {
  const taskRunData = await parseMessage(context, transaction, preprocessedMessage)
  // As the forecast and approved indicators are booleans progression must be based on them being defined.
  if (taskRunData.taskRunCompletionTime && taskRunData.workflowId && taskRunData.taskRunId &&
    typeof taskRunData.forecast !== 'undefined' && typeof taskRunData.approved !== 'undefined') {
    await processTaskRunDataIfPossible(context, transaction, taskRunData)
  }
}

async function processTaskRunDataIfPossible (context, transaction, taskRunData) {
  // Store the task run ID in the cpntext so it can be logged if scheduled messages
  // have to be published manually.
  context.taskRunId = taskRunData.taskRunId
  // Do not import out of date forecast data.
  if (!taskRunData.forecast || await isLatestTaskRunForWorkflow(context, taskRunData)) {
    await processTaskRunData(context, transaction, taskRunData)
  } else {
    logObsoleteTaskRunMessage(context, taskRunData)
  }
}

async function processTaskRunData (context, transaction, taskRunData) {
  const ignoredWorkflow =
    await isIgnoredWorkflow(context, taskRunData)

  if (ignoredWorkflow) {
    context.log(`${taskRunData.workflowId} is an ignored workflow`)
  } else {
    await executePreparedStatementInTransaction(isSpanWorkflow, context, transaction, taskRunData)
    await buildAndProcessOutgoingWorkflowMessagesIfPossible(context, transaction, taskRunData)
  }
}

async function processOutgoingMessagesIfPossible (context, taskRunData) {
  // Expect a number of plots/filters in message format, to forward to next function
  if (taskRunData.outgoingMessages.length > 0) {
    await createTimeseriesHeaderIfNeeded(context, taskRunData)
    await deactivateObsoleteStagingExceptionsBySourceFunctionAndWorkflowId(context, taskRunData)
    await deactivateStagingExceptionBySourceFunctionAndTaskRunId(context, taskRunData)
    await deactivateTimeseriesStagingExceptionsForNonExistentTaskRunPlotsAndFilters(context, taskRunData)
    context.log(`Completed exception deactivation processing for task run ${taskRunData.taskRunId} of workflow ${taskRunData.workflowId}`)

    // Throw an exception to cause attempted message replay if the PI Server is offline
    // or it can be determined that all data for the task run is not available from the
    // PI Server yet. The message is eligible for replay a certain number of times before
    // being placed on a dead letter queue.
    await checkIfPiServerHasAllDataForTaskRunIfPossible(context, taskRunData)
    // Prepare to send a message for each plot/filter associated with the task run.
    context.bindings.importFromFews = taskRunData.outgoingMessages
  } else {
    await processReasonForNoOutgoingMessages(context, taskRunData)
  }
}

async function parseMessage (context, transaction, message) {
  const taskRunData = {
    message,
    transaction,
    throwStagingErrorFollowingStagingExceptionCreation: true,
    sourceFunction: 'P',
    outgoingMessages: [],
    timeseriesStagingErrors: [],
    unprocessedItems: [],
    itemsEligibleForReplay: []
  }

  taskRunData.taskRunId = await getTaskRunId(context, taskRunData)
  taskRunData.workflowId = await getWorkflowId(context, taskRunData)
  taskRunData.sourceDetails = `task run ${taskRunData.taskRunId}`
  taskRunData.timeseriesHeaderExistsForTaskRun = await doesTimeseriesHeaderExistForTaskRun(context, taskRunData)
  // The core engine uses UTC but does not appear to use ISO 8601 date formatting. As such dates need to be specified as
  // UTC using ISO 8601 date formatting manually to ensure portability between local and cloud environments.
  taskRunData.taskRunStartTime =
    moment(new Date(`${await getTaskRunStartDate(context, taskRunData)} UTC`)).toISOString()
  taskRunData.taskRunCompletionTime =
    moment(new Date(`${await getTaskRunCompletionDate(context, taskRunData)} UTC`)).toISOString()
  taskRunData.forecast = await isForecast(context, taskRunData)
  taskRunData.approved = await isTaskRunApproved(context, taskRunData)

  return taskRunData
}

async function processMessage (transaction, context, message) {
  try {
    // If a JSON message is received convert it to a string.
    const preprocessedMessage = await preprocessMessage(context, transaction, message)
    if (preprocessedMessage) {
      await parseMessageAndProcessTaskRunDataIfPossible(context, transaction, preprocessedMessage)
    }
  } catch (err) {
    if (err instanceof PartialFewsDataError) {
      processPartialFewsDataError(err.context, err.incomingMessage, 'processFewsEventCode')
    } else if (!(err instanceof StagingError)) {
      // A StagingError is thrown when message replay is not possible without manual intervention.
      // In this case a staging exception record has been created and the message should be consumed.
      // Propagate other errors to facilitate message replay.
      throw err
    }
  }
}

async function createTimeseriesHeaderIfNeeded (context, taskRunData) {
  if (!taskRunData.timeseriesHeaderExistsForTaskRun) {
    await createTimeseriesHeader(context, taskRunData)
  }
}

async function processReasonForNoOutgoingMessages (context, taskRunData) {
  if (taskRunData.timeseriesHeaderExistsForTaskRun) {
    // If there is a taskRun header, this taskRun has partially/successfully run at least once before
    // If there are no messages to send out this means:
    // - all the plots/filters for the workflow taskRun have already been loaded successfully into timeseries
    // - there was a partial/failed previous load AND the workflow reference data associated with the missing timeseries (t-s-exceptions) has not yet been refreshed
    context.log(`Ignoring message for task run ${taskRunData.taskRunId} - No plots/filters require processing`)
  } else if ((taskRunData.forecast && !taskRunData.approved) || taskRunData.itemsToBeProcessed.length > 0) {
    // If there is no header this means this taskRun has not partially/successfully run before.
    // In this case (no header) the function app will find and store all corresponding plots/filters as items to be processed for a taskRun.
    // If there are itemsToBeProcessed then there IS reference data in staging for the taskRun/workflow.
    // No messages at this point means the items to process are plots linked to an unapproved forecast and so should not be forwarded
    context.log(`All plots in the taskRun: ${taskRunData.taskRunId} (for workflowId: ${taskRunData.workflowId}) are unapproved.`)
  } else {
    // If this code is reached there are no items to process, messages to forward or header row for observed data or approved forecast data meaning:
    // - this taskRun has not partially/successfully run before.
    // - staging has no reference data listed for the taskRun workflowId
    taskRunData.errorMessage = `Missing PI Server input data for ${taskRunData.workflowId}`
    await createStagingException(context, taskRunData)
  }
}
