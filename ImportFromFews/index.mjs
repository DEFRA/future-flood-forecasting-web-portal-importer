import buildPiServerGetTimeseriesDisplayGroupFallbackUrlIfPossible from './helpers/build-pi-server-get-timeseries-display-groups-fallback-url-if-possible.js'
import buildPiServerGetTimeseriesDisplayGroupUrlIfPossible from './helpers/build-pi-server-get-timeseries-display-groups-url-if-possible.js'
import buildPiServerGetTimeseriesUrlIfPossible from './helpers/build-pi-server-get-timeseries-url-if-possible.js'
import createStagingException from '../Shared/timeseries-functions/create-staging-exception.js'
import deactivateObsoleteTimeseriesStagingExceptionsForWorkflowPlotOrFilter from './helpers/deactivate-obsolete-timeseries-staging-exceptions-for-workflow-plot-or-filter.js'
import deactivateObsoleteStagingExceptionsBySourceFunctionAndWorkflowId from '../Shared/timeseries-functions/deactivate-obsolete-staging-exceptions-by-source-function-and-workflow-id.js'
import deactivateStagingExceptionBySourceFunctionAndTaskRunIdIfPossible from './helpers/deactivate-staging-exceptions-by-source-function-and-task-run-id-if-possible.js'
import { doInTransaction, executePreparedStatementInTransaction } from '../Shared/transaction-helper.js'
import getTimeseriesHeaderData from './helpers/get-timeseries-header-data.js'
import { minifyAndGzip } from '../Shared/utils.js'
import isLatestTaskRunForWorkflow from '../Shared/timeseries-functions/is-latest-task-run-for-workflow.js'
import processTaskRunDataForNonForecastOrLatestTaskRunForWorkflowIfPossible from './helpers/process-task-run-data-for-non-forecast-or-latest-task-run-for-workflow-if-possible.js'
import isMessageIgnored from './helpers/is-message-ignored.js'
import isSpanWorkflow from '../Shared/timeseries-functions/check-spanning-workflow.js'
import retrieveAndLoadFewsData from ('./helpers/retrieve-and-load-fews-data')


export default async function (context, message) {
  context.log(`Processing timeseries import message: ${JSON.stringify(message)}`)
  const errorMessage = 'The FEWS data import function has failed with the following error:'
  const isolationLevel = null

  const taskRunData = Object.assign({}, message)
  await doInTransaction({ fn: processMessage, context, errorMessage, isolationLevel }, message, taskRunData)
  // If all plots/filters for the task run have been processed, associated staging exceptions can be deactivated.
  // This is performed in a new transaction to avoid deadlocks when plots/filters are processed concurrently.
  await doInTransaction({ fn: deactivateStagingExceptionBySourceFunctionAndTaskRunIdIfPossible, context, errorMessage, isolationLevel }, taskRunData)
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

  if (message.taskRunId && (!!message.plotId || !!message.filterId) && !(!!message.plotId && !!message.filterId)) {
    await getTimeseriesHeaderData(context, taskRunData)
    await setSourceConfig(taskRunData)
    await processMessageIfPossible(taskRunData, context, message)
  } else {
    taskRunData.errorMessage = 'Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes'
    await createStagingException(context, taskRunData)
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
