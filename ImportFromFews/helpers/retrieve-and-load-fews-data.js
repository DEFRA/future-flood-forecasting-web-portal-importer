const axios = require('axios')
const sql = require('mssql')
const deactivateTimeseriesStagingExceptionsForTaskRunPlotOrFilter = require('./deactivate-timeseries-staging-exceptions-for-task-run-plot-or-filter')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const getPiServerErrorMessage = require('../../Shared/timeseries-functions/get-pi-server-error-message')
const { prepareFewsDataForImport } = require('../../Shared/utils')
const processImportError = require('./process-import-error')

module.exports = async function (context, taskRunData) {
  await retrieveFewsData(context, taskRunData)
  if (taskRunData.fewsData) {
    await executePreparedStatementInTransaction(loadFewsData, context, taskRunData.transaction, taskRunData)
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
      !Object.is(buildPiServerUrlCall.fewsPiUrl, undefined) && (await retrieveFewsDataFromFewsPiServer(context, taskRunData, buildPiServerUrlCall))
      // Fews data has been retrieved and compressed or the URL could not be built. Do not make any more calls to the PI Server.
      break
    } catch (err) {
      await processDataRetrievalError(context, taskRunData, err)
    }
  }
  await processFewsDataRetrievalResults(context, taskRunData)
}

async function retrieveFewsDataFromFewsPiServer (context, taskRunData, buildPiServerUrlCall) {
  await retrieveAndCompressFewsData(context, taskRunData)
  taskRunData.fewsParameters = buildPiServerUrlCall.fewsParameters
  taskRunData.fewsPiUrl = buildPiServerUrlCall.fewsPiUrl
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
  await logTaskRunProgress(context, taskRunData, 'Retrieving data')
  const fewsResponse = await axios(axiosConfig)
  await logTaskRunProgress(context, taskRunData, 'Retrieved data')
  taskRunData.fewsData = await prepareFewsDataForImport(fewsResponse.data, taskRunData)
  await logTaskRunProgress(context, taskRunData, 'Compressed data')
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
  await logTaskRunProgress(context, taskRunData, 'Loading data')
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
  await logTaskRunProgress(context, taskRunData, 'Loaded data')
}

async function logTaskRunProgress (context, taskRunData, messageContext) {
  context.log(`${messageContext} for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
}
