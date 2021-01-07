const axios = require('axios')
const sql = require('mssql')
const buildPiServerGetTimeseriesDisplayGroupFallbackUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-display-groups-fallback-url-if-possible')
const buildPiServerGetTimeseriesDisplayGroupUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-display-groups-url-if-possible')
const buildPiServerGetTimeseriesUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-url-if-possible')
const createStagingException = require('../Shared/timeseries-functions/create-staging-exception')
const createTimeseriesStagingException = require('./helpers/create-timeseries-staging-exception')
const deactivateTimeseriesStagingExceptionsForTaskRunPlotOrFilter = require('./helpers/deactivate-timeseries-staging-exceptions-for-task-run-plot-or-filter')
const deactivateObsoleteTimeseriesStagingExceptionsForWorkflowPlotOrFilter = require('./helpers/deactivate-obsolete-timeseries-staging-exceptions-for-workflow-plot-or-filter')
const deactivateObsoleteStagingExceptionsBySourceFunctionAndWorkflowId = require('../Shared/timeseries-functions/deactivate-obsolete-staging-exceptions-by-source-function-and-workflow-id')
const deactivateStagingExceptionBySourceFunctionAndTaskRunIdIfPossible = require('./helpers/deactivate-staging-exceptions-by-source-function-and-task-run-id-if-possible')
const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const getPiServerErrorMessage = require('../Shared/timeseries-functions/get-pi-server-error-message')
const getTimeseriesHeaderData = require('./helpers/get-timeseries-header-data')
const { minifyAndGzip } = require('../Shared/utils')
const isLatestTaskRunForWorkflow = require('../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const isMessageIgnored = require('./helpers/is-message-ignored')
const isSpanWorkflow = require('../Shared/timeseries-functions/check-spanning-workflow')
const TimeseriesStagingError = require('../Shared/timeseries-functions/timeseries-staging-error')

module.exports = async function (context, message) {
  context.log(`Processing timeseries import message: ${JSON.stringify(message)}`)
  await doInTransaction(processMessage, context, 'The FEWS data import function has failed with the following error:', null, message)
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

async function processMessage (transaction, context, message) {
  const taskRunData = Object.assign({}, message)
  taskRunData.transaction = transaction
  taskRunData.message = message
  taskRunData.sourceFunction = 'I'
  taskRunData.getAllLocationsForWorkflowPlotWhenNoTimeseriesExist = true

  if (message.taskRunId) {
    await getTimeseriesHeaderData(context, taskRunData)
  }

  if (message.taskRunId &&
       ((!!message.plotId || !!message.filterId) && !(!!message.plotId && !!message.filterId))) {
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
    } else {
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
    await processMessageIfPossible(taskRunData, context, message)
  } else {
    taskRunData.errorMessage = 'Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes'
    await createStagingException(context, taskRunData)
  }
}

async function processDataRetrievalError (context, taskRunData, err) {
  if (err.response && err.response.status === 400) {
    const piServerErrorMessage = await getPiServerErrorMessage(context, err)
    context.log.warn(`Bad request made to PI Server for (${piServerErrorMessage}) ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
    const buildPiServerUrlCall = taskRunData.buildPiServerUrlCalls[taskRunData.piServerUrlCallsIndex]
    // A bad request was made to the PI server. Store the error as it might be needed to create a
    // timeseries staging exception once all data retrieval attempts from the PI server have been made.
    buildPiServerUrlCall.error = err
  } else {
    throw err
  }
}

async function processImportError (context, taskRunData, err) {
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
      const csvError = (err.response && err.response.status === 400) || false
      const csvType = csvError ? taskRunData.csvType : null
      const piServerErrorMessage = await getPiServerErrorMessage(context, err)
      const errorDescription = `An error occurred while processing data for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}): ${piServerErrorMessage}`
      errorData = {
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
    await createTimeseriesStagingException(context, errorData)
  }
}

async function importFromFews (context, taskRunData) {
  try {
    if (!taskRunData.forecast || await isLatestTaskRunForWorkflow(context, taskRunData)) {
      await retrieveFewsData(context, taskRunData)
      if (taskRunData.fewsData) {
        await executePreparedStatementInTransaction(loadFewsData, context, taskRunData.transaction, taskRunData)
        await deactivateStagingExceptionBySourceFunctionAndTaskRunIdIfPossible(context, taskRunData)
      }
    } else {
      context.log.warn(`Ignoring message for plot ${taskRunData.plotId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) completed on ${taskRunData.taskRunCompletionTime}` +
        ` - ${taskRunData.latestTaskRunId} completed on ${taskRunData.latestTaskRunCompletionTime} is the latest task run for workflow ${taskRunData.workflowId}`)
    }
    if (taskRunData.forecast && await isLatestTaskRunForWorkflow(context, taskRunData)) {
      await deactivateObsoleteStagingExceptionsBySourceFunctionAndWorkflowId(context, taskRunData)
      await deactivateObsoleteTimeseriesStagingExceptionsForWorkflowPlotOrFilter(context, taskRunData)
    }
  } catch (err) {
    await processImportError(context, taskRunData, err)
  }
}

async function retrieveFewsData (context, taskRunData) {
  // Iterate through the available functions for retrieving data from the PI server until one
  // succeeds or the set of available functions is exhausted.
  for (const index in taskRunData.buildPiServerUrlCalls) {
    taskRunData.piServerUrlCallsIndex = index
    const buildPiServerUrlCall = taskRunData.buildPiServerUrlCalls[index]
    try {
      await buildPiServerUrlCall.buildPiServerUrlIfPossibleFunction(context, taskRunData)
      if (buildPiServerUrlCall.fewsPiUrl) {
        await retrieveAndCompressFewsData(context, taskRunData)
        taskRunData.fewsParameters = buildPiServerUrlCall.fewsParameters
        taskRunData.fewsPiUrl = buildPiServerUrlCall.fewsPiUrl
        // Fews data has been retrieved and compressed so no further calls to the PI Server are needed.
        break
      } else {
        // The URL could not be built. Do not make any more calls to the PI Server.
        break
      }
    } catch (err) {
      await processDataRetrievalError(context, taskRunData, err)
    }
  }
  await processFewsDataRetrievalResults(context, taskRunData)
}

async function retrieveAndCompressFewsData (context, taskRunData) {
  const axiosConfig = {
    method: 'get',
    url: taskRunData.buildPiServerUrlCalls[taskRunData.piServerUrlCallsIndex].fewsPiUrl,
    responseType: 'stream',
    headers: {
      Accept: 'application/json'
    }
  }
  context.log(`Retrieving data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  const fewsResponse = await axios(axiosConfig)
  context.log(`Retrieved data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  taskRunData.fewsData = await minifyAndGzip(fewsResponse.data)
  context.log(`Compressed data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
}

async function processFewsDataRetrievalResults (context, taskRunData) {
  // Deactivate previous timeseries staging exceptions for the plot/filter of the task run.
  // If the current attempt to process the plot/filter succeeds with no errors all is well.
  // If the current attempt to process the plot/filter does not succeed a new timeseries staging exception will be created.
  await deactivateTimeseriesStagingExceptionsForTaskRunPlotOrFilter(context, taskRunData)
  if (taskRunData.buildPiServerUrlCalls[0].error) {
    // If the original call to the PI server caused an error create a timeseries staging exception.
    await processImportError(context, taskRunData, taskRunData.buildPiServerUrlCalls[0].error)
  }
}

async function createStagedTimeseriesMessageIfNeeded (context, timeseriesId) {
  const bindingDefinitions = JSON.stringify(context.bindingDefinitions)
  bindingDefinitions.includes('"direction":"out"') ? context.bindings.stagedTimeseries = [] : context.log('No output binding attached.')

  if (bindingDefinitions.includes('"direction":"out"')) {
    // Prepare to send a message containing the primary key of the inserted record.
    context.bindings.stagedTimeseries.push({
      id: timeseriesId
    })
  }
}

async function loadFewsData (context, preparedStatement, taskRunData) {
  context.log(`Loading data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  await preparedStatement.input('fewsData', sql.VarBinary)
  await preparedStatement.input('fewsParameters', sql.NVarChar)
  await preparedStatement.input('timeseriesHeaderId', sql.NVarChar)
  await preparedStatement.output('insertedId', sql.UniqueIdentifier)
  await preparedStatement.prepare(`
    insert into
      fff_staging.timeseries (fews_data, fews_parameters, timeseries_header_id)
    output
      inserted.id
    values
      (@fewsData, @fewsParameters, @timeseriesHeaderId)
  `)

  const parameters = {
    fewsData: taskRunData.fewsData,
    fewsParameters: taskRunData.fewsParameters,
    timeseriesHeaderId: taskRunData.timeseriesHeaderId
  }

  const result = await preparedStatement.execute(parameters)
  if (result.recordset && result.recordset[0] && result.recordset[0].id) {
    createStagedTimeseriesMessageIfNeeded(context, result.recordset && result.recordset[0] && result.recordset[0].id)
  }
  context.log(`Loaded data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
}
