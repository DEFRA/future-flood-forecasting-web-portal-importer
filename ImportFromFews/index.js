const axios = require('axios')
const sql = require('mssql')
const createTimeseriesStagingException = require('./helpers/create-timeseries-staging-exception')
const buildPiServerGetTimeseriesUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-url-if-possible')
const buildPiServerGetTimeseriesDisplayGroupUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-display-groups-url-if-possible')
const { gzip } = require('../Shared/utils')
const isSpanWorkflow = require('../ImportFromFews/helpers/check-spanning-workflow')
const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const createStagingException = require('../Shared/timeseries-functions/create-staging-exception')
const getTimeseriesHeaderData = require('./helpers/get-timeseries-header-data')
const isMessageIgnored = require('./helpers/is-message-ignored')
const isLatestTaskRunForWorkflow = require('../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const getPiServerErrorMessage = require('../Shared/timeseries-functions/get-pi-server-error-message')
const TimeseriesStagingError = require('./helpers/timeseries-staging-error')

module.exports = async function (context, message) {
  context.log(`Processing timeseries import message: ${JSON.stringify(message)}`)
  await doInTransaction(processMessage, context, 'The FEWS data import function has failed with the following error:', null, message)
}

async function processMessageIfPossible (taskRunData, context, message) {
  await executePreparedStatementInTransaction(getTimeseriesHeaderData, context, taskRunData.transaction, taskRunData)
  if (taskRunData.timeseriesHeaderId) {
    if (!(await isMessageIgnored(context, taskRunData))) {
      await executePreparedStatementInTransaction(isSpanWorkflow, context, taskRunData.transaction, taskRunData)
      await importFromFews(context, taskRunData)
    }
  } else {
    taskRunData.errorMessage = `Unable to retrieve TIMESERIES_HEADER record for task run ${message.taskRunId}`
    await executePreparedStatementInTransaction(createStagingException, context, taskRunData.transaction, taskRunData)
  }
}

async function processMessage (transaction, context, message) {
  const taskRunData = Object.assign({}, message)
  taskRunData.message = message
  if (message.taskRunId &&
       ((!!message.plotId || !!message.filterId) && !(!!message.plotId && !!message.filterId))) {
    taskRunData.transaction = transaction
    if (taskRunData.plotId) {
      taskRunData.sourceId = taskRunData.plotId
      taskRunData.sourceType = 'P'
      taskRunData.sourceTypeDescription = 'plot'
      taskRunData.buildPiServerUrlIfPossibleFunction = buildPiServerGetTimeseriesDisplayGroupUrlIfPossible
      // Set the CSV type as unknown until the CSV file containing the plot can be found.
      taskRunData.csvType = 'U'
    } else {
      taskRunData.sourceId = taskRunData.filterId
      taskRunData.sourceType = 'F'
      taskRunData.sourceTypeDescription = 'filter'
      taskRunData.buildPiServerUrlIfPossibleFunction = buildPiServerGetTimeseriesUrlIfPossible
      taskRunData.csvType = 'N'
    }
    await processMessageIfPossible(taskRunData, context, message)
  } else {
    taskRunData.errorMessage = 'Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes'
    await executePreparedStatementInTransaction(createStagingException, context, transaction, taskRunData)
  }
}

async function processImportError (context, taskRunData, err) {
  let errorData
  if (!(err instanceof TimeseriesStagingError) && typeof err.response === 'undefined') {
    context.log.error(`Failed to connect to ${process.env['FEWS_PI_API']}`)
    throw err
  } else {
    if (err instanceof TimeseriesStagingError) {
      errorData = err.context
    } else {
      const csvError = (err.response && err.response.status === 400) || false
      const csvType = csvError ? taskRunData.csvType : null
      const piServerErrorMessage = await getPiServerErrorMessage(context, err)
      const errorDescription = `An error occured while processing data for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}): ${piServerErrorMessage}`
      errorData = {
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
    await executePreparedStatementInTransaction(createTimeseriesStagingException, context, taskRunData.transaction, errorData)
  }
}

async function importFromFews (context, taskRunData) {
  try {
    if (!taskRunData.forecast || await executePreparedStatementInTransaction(isLatestTaskRunForWorkflow, context, taskRunData.transaction, taskRunData)) {
      await taskRunData['buildPiServerUrlIfPossibleFunction'](context, taskRunData)
      if (taskRunData.fewsPiUrl) {
        await retrieveFewsData(context, taskRunData)
        await executePreparedStatementInTransaction(loadFewsData, context, taskRunData.transaction, taskRunData)
      }
    } else {
      context.log.warn(`Ignoring message for plot ${taskRunData.plotId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) completed on ${taskRunData.taskRunCompletionTime}` +
        ` - ${taskRunData.latestTaskRunId} completed on ${taskRunData.latestTaskRunCompletionTime} is the latest task run for workflow ${taskRunData.workflowId}`)
    }
  } catch (err) {
    await processImportError(context, taskRunData, err)
  }
}

async function retrieveFewsData (context, taskRunData) {
  const axiosConfig = {
    method: 'get',
    url: taskRunData.fewsPiUrl,
    responseType: 'stream',
    headers: {
      Accept: 'application/json'
    }
  }
  context.log(`Retrieving data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  const fewsResponse = await axios(axiosConfig)
  context.log(`Retrieved data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  taskRunData.fewsData = await gzip(fewsResponse.data)
  context.log(`Compressed data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
}

async function createStagedTimeseriesMessageIfNeeded (context, timeseriesId) {
  const bindingDefinitions = JSON.stringify(context.bindingDefinitions)
  bindingDefinitions.includes(`"direction":"out"`) ? context.bindings.stagedTimeseries = [] : context.log(`No output binding attached.`)

  if (bindingDefinitions.includes(`"direction":"out"`)) {
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
