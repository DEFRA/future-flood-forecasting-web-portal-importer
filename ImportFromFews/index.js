const buildPiServerGetTimeseriesDisplayGroupFallbackUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-display-groups-fallback-url-if-possible')
const buildPiServerGetTimeseriesDisplayGroupUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-display-groups-url-if-possible')
const buildPiServerGetTimeseriesUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-url-if-possible')
const createStagingException = require('../Shared/timeseries-functions/create-staging-exception')
const deactivateObsoleteTimeseriesStagingExceptionsForWorkflowPlotOrFilter = require('./helpers/deactivate-obsolete-timeseries-staging-exceptions-for-workflow-plot-or-filter')
const deactivateObsoleteStagingExceptionsBySourceFunctionAndWorkflowId = require('../Shared/timeseries-functions/deactivate-obsolete-staging-exceptions-by-source-function-and-workflow-id')
const deactivateStagingExceptionBySourceFunctionAndTaskRunIdIfPossible = require('./helpers/deactivate-staging-exceptions-by-source-function-and-task-run-id-if-possible')
const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const getTimeseriesHeaderData = require('./helpers/get-timeseries-header-data')
const isLatestTaskRunForWorkflow = require('../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const isMessageIgnored = require('./helpers/is-message-ignored')
const processTaskRunDataForNonForecastOrLatestTaskRunForWorkflowIfPossible = require('./helpers/process-task-run-data-for-non-forecast-or-latest-task-run-for-workflow-if-possible')
const isSpanWorkflow = require('../Shared/timeseries-functions/check-spanning-workflow')
const processImportError = require('./helpers/process-import-error')
const retrieveAndLoadFewsData = require('./helpers/retrieve-and-load-fews-data')
const PartialFewsDataError = require('../Shared/message-replay/partial-fews-data-error')
const processPartialFewsDataError = require('../Shared/message-replay/process-partial-fews-data-error')
const publishScheduledMessagesIfNeeded = require('../Shared/timeseries-functions/publish-scheduled-messages-if-needed')

module.exports = async function (context, message) {
  context.log(`Processing timeseries import message: ${JSON.stringify(message)}`)
  const errorMessage = 'The FEWS data import function has failed with the following error:'
  const isolationLevel = null

  const taskRunData = Object.assign({}, message)
  await doInTransaction({ fn: processMessage, context, errorMessage, isolationLevel }, message, taskRunData)
  // If all plots/filters for the task run have been processed, associated staging exceptions can be deactivated.
  // This is performed in a new transaction to avoid deadlocks when plots/filters are processed concurrently.
  await doInTransaction({ fn: deactivateStagingExceptionBySourceFunctionAndTaskRunIdIfPossible, context, errorMessage, isolationLevel }, taskRunData)

  // If context.bindings.importFromFews exists, this indicates that the
  // plot/filter being processed has missing events. This could be due
  // to incomplete PI Server indexing for the task run or there could be
  // genuine missing events. Without further information to determine
  // if the missing events are genuine, the message needs to be replayed.
  // Message replay will be attempted until either of the following occur:
  // - There are no missing events for the plot/filter.
  // - The maximum amount of time allowed for PI Server indexing to complete
  //   is exceeded. If events are still missing at this time, available data
  //   for the plot/filter will be loaded. This scenario will always occur
  //   for genuine missing events and will result in delayed loading accordingly.
  const scheduledMessageConfig = {
    destinationName: 'fews-import-queue',
    outputBinding: 'importFromFews'
  }

  // In common with messages published using context bindings, publish scheduled messages outside of the
  // transactions used during message processing.
  await publishScheduledMessagesIfNeeded(context, scheduledMessageConfig)
}

async function processMessageIfPossible (taskRunData, context, message) {
  if (taskRunData.timeseriesHeaderId) {
    if (!(await isMessageIgnored(context, taskRunData))) {
      await executePreparedStatementInTransaction(isSpanWorkflow, context, taskRunData.transaction, taskRunData)
      await importFromFews(context, taskRunData)
    }
  } else {
    taskRunData.errorMessage = `Unable to retrieve TIMESERIES_HEADER record for task run ${message.taskRunId}`
    await createStagingException(context, taskRunData)
  }
}

async function processMessage (transaction, context, message, taskRunData) {
  taskRunData.transaction = transaction
  taskRunData.message = message
  taskRunData.sourceFunction = 'I'
  taskRunData.getAllLocationsForWorkflowPlotWhenNoTimeseriesExist = true

  try {
    if (message.taskRunId && (!!message.plotId || !!message.filterId) && !(!!message.plotId && !!message.filterId)) {
      await getTimeseriesHeaderData(context, taskRunData)
      await setSourceConfig(taskRunData)
      await processMessageIfPossible(taskRunData, context, message)
    } else {
      taskRunData.errorMessage =
        'Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes'
      await createStagingException(context, taskRunData)
    }
  } catch (err) {
    if (err instanceof PartialFewsDataError) {
      processPartialFewsDataError(err.context, err.incomingMessage, 'importFromFews')
    } else {
      throw err
    }
  }
}

async function setSourceConfig (taskRunData) {
  if (taskRunData.plotId) {
    taskRunData.sourceId = taskRunData.plotId
    taskRunData.sourceType = 'P'
    taskRunData.sourceTypeDescription = 'plot'
    taskRunData.buildPiServerUrlCalls = [
      {
        buildPiServerUrlIfPossibleFunction: buildPiServerGetTimeseriesDisplayGroupUrlIfPossible
      },
      // A fallback function that attempts to remove problematic locations from the original PI Server call
      // if the original PI Server call fails.
      {
        buildPiServerUrlIfPossibleFunction: buildPiServerGetTimeseriesDisplayGroupFallbackUrlIfPossible
      }
    ]
    // Set the CSV type as unknown until the CSV file containing the plot can be found.
    taskRunData.csvType = 'U'
  } else if (taskRunData.filterId) {
    taskRunData.sourceId = taskRunData.filterId
    taskRunData.sourceType = 'F'
    taskRunData.sourceTypeDescription = 'filter'
    taskRunData.buildPiServerUrlCalls = [
      {
        buildPiServerUrlIfPossibleFunction: buildPiServerGetTimeseriesUrlIfPossible
      }
    ]
    taskRunData.csvType = 'N'
  }
  taskRunData.sourceDetails = `${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`
}

async function importFromFews (context, taskRunData) {
  try {
    await processTaskRunDataForNonForecastOrLatestTaskRunForWorkflowIfPossible(context, taskRunData, false, retrieveAndLoadFewsData)

    if (taskRunData.forecast && await isLatestTaskRunForWorkflow(context, taskRunData)) {
      await deactivateObsoleteStagingExceptionsBySourceFunctionAndWorkflowId(context, taskRunData)
      await deactivateObsoleteTimeseriesStagingExceptionsForWorkflowPlotOrFilter(context, taskRunData)
    }
  } catch (err) {
    await processImportError(context, taskRunData, err)
  }
}
