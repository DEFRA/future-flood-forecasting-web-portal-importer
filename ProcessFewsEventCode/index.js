const moment = require('moment')
const createStagingException = require('../Shared/timeseries-functions/create-staging-exception')
const createTimeseriesHeader = require('./helpers/create-timeseries-header')
const doesTimeseriesHeaderExistForTaskRun = require('./helpers/does-timeseries-header-exist-for-task-run')
const { doInTransaction } = require('../Shared/transaction-helper')
const isForecast = require('./helpers/is-forecast')
const isIgnoredWorkflow = require('../Shared/timeseries-functions/is-ignored-workflow')
const isLatestTaskRunForWorkflow = require('../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const checkIfPiServerIsOnline = require('./helpers/check-if-pi-server-is-online')
const isTaskRunApproved = require('./helpers/is-task-run-approved')
const getTaskRunCompletionDate = require('./helpers/get-task-run-completion-date')
const getTaskRunId = require('./helpers/get-task-run-id')
const getTaskRunPlotsAndFiltersToBeProcessed = require('./helpers/get-task-run-plots-and-filters-to-be-processed')
const getTaskRunStartDate = require('./helpers/get-task-run-start-date')
const getWorkflowId = require('./helpers/get-workflow-id')
const preprocessMessage = require('./helpers/preprocess-message')
const StagingError = require('../Shared/timeseries-functions/staging-error')

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
  const messageToLog = typeof message === 'string' ? message : JSON.stringify(message)
  context.log(`Processing core engine message: ${messageToLog}`)
  await doInTransaction(processMessage, context, 'The message routing function has failed with the following error:', null, message)
  // context.done() not required in async functions
}

async function buildAndProcessOutgoingWorkflowMessagesIfPossible (context, taskRunData) {
  // Process data for each CSV file associated with the workflow.
  await getTaskRunPlotsAndFiltersToBeProcessed(context, taskRunData)
  const itemsToBeProcessed = taskRunData.unprocessedItems.concat(taskRunData.itemsEligibleForReplay)
  // Create a message for each plot/filter to be processed for the task run.
  for (const itemToBeProcessed of itemsToBeProcessed) {
    if (itemToBeProcessed.sourceType === 'P' && taskRunData.forecast && !taskRunData.approved) {
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
  await processOutgoingMessagesIfPossible(context, taskRunData)
}

async function processTaskRunData (context, taskRunData) {
  const ignoredWorkflow =
    await isIgnoredWorkflow(context, taskRunData)

  if (ignoredWorkflow) {
    context.log(`${taskRunData.workflowId} is an ignored workflow`)
  } else {
    await buildAndProcessOutgoingWorkflowMessagesIfPossible(context, taskRunData)
  }
}

async function processOutgoingMessagesIfPossible (context, taskRunData) {
  if (taskRunData.outgoingMessages.length > 0) {
    if (!taskRunData.timeseriesHeaderExistsForTaskRun) {
      await createTimeseriesHeader(context, taskRunData)
    }

    // If the PI Server is offline an exception is thrown. The message is  eligible for replay a certain number of times before
    // being placed on a dead letter queue.
    await checkIfPiServerIsOnline(context)
    // Prepare to send a message for each plot/filter associated with the task run.
    context.bindings.importFromFews = taskRunData.outgoingMessages
  } else if (taskRunData.timeseriesHeaderExistsForTaskRun) {
    context.log(`Ignoring message for task run ${taskRunData.taskRunId} - No plots/filters require processing`)
  } else {
    taskRunData.errorMessage = `Missing PI Server input data for ${taskRunData.workflowId}`
    await createStagingException(context, taskRunData)
  }
}

async function parseMessage (context, transaction, message) {
  const taskRunData = {
    message: message,
    transaction: transaction,
    throwStagingErrorFollowingStagingExceptionCreation: true,
    sourceFunction: 'P',
    outgoingMessages: [],
    timeseriesStagingErrors: [],
    unprocessedItems: [],
    itemsEligibleForReplay: []
  }
  taskRunData.taskRunId = await getTaskRunId(context, taskRunData)
  taskRunData.workflowId = await getWorkflowId(context, taskRunData)
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
      const taskRunData = await parseMessage(context, transaction, preprocessedMessage)
      // As the forecast and approved indicators are booleans progression must be based on them being defined.
      if (taskRunData.taskRunCompletionTime && taskRunData.workflowId && taskRunData.taskRunId &&
        typeof taskRunData.forecast !== 'undefined' && typeof taskRunData.approved !== 'undefined') {
        // Do not import out of date forecast data.
        if (!taskRunData.forecast || await isLatestTaskRunForWorkflow(context, taskRunData)) {
          await processTaskRunData(context, taskRunData)
        } else {
          context.log.warn(`Ignoring message for task run ${taskRunData.taskRunId} completed on ${taskRunData.taskRunCompletionTime}` +
            ` - ${taskRunData.latestTaskRunId} completed on ${taskRunData.latestTaskRunCompletionTime} is the latest task run for workflow ${taskRunData.workflowId}`)
        }
      }
    }
  } catch (err) {
    if (!(err instanceof StagingError)) {
      // A StagingError is thrown when message replay is not possible without manual intervention.
      // In this case a staging exception record has been created and the message should be consumed.
      // Propagate other errors to facilitate message replay.
      throw err
    }
  }
}
