const axios = require('axios')
const sql = require('mssql')
const createTimeseriesStagingException = require('./helpers/create-timeseries-staging-exception')
const buildPiServerGetTimeseriesUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-url-if-possible')
const buildPiServerGetTimeseriesDisplayGroupUrlIfPossible = require('./helpers/build-pi-server-get-timeseries-display-groups-url-if-possible')
const { gzip } = require('../Shared/utils')
const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const createStagingException = require('../Shared/timeseries-functions/create-staging-exception')
const doTimeseriesExistForTaskRunPlotOrFilter = require('./helpers/do-timeseries-exist-for-task-run-plot-or-filter')
const doTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter = require('./helpers/do-timeseries-staging-exceptions-exist-for-task-run-plot-or-filter')
const getTimeseriesHeaderData = require('./helpers/get-timeseries-header-data')
const isIgnoredWorkflow = require('../Shared/timeseries-functions/is-ignored-workflow')
const getPiServerErrorMessage = require('../Shared/timeseries-functions/get-pi-server-error-message')
const TimeseriesStagingError = require('./helpers/timeseries-staging-error')

module.exports = async function (context, message) {
  await doInTransaction(processMessage, context, 'The FEWS data import function has failed with the following error:', null, message)
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

    await executePreparedStatementInTransaction(getTimeseriesHeaderData, context, taskRunData.transaction, taskRunData)

    if (taskRunData.timeseriesHeaderId) {
      if (await executePreparedStatementInTransaction(isIgnoredWorkflow, context, taskRunData.transaction, taskRunData.workflowId)) {
        context.log(`${taskRunData.workflowId} is an ignored workflow`)
      } else {
        const timeseriesExistForTaskRunPlotOrFilter =
          await executePreparedStatementInTransaction(doTimeseriesExistForTaskRunPlotOrFilter, context, taskRunData.transaction, taskRunData)

        const timeseriesStagingExceptionsExistForTaskRunPlotOrFilter =
          await executePreparedStatementInTransaction(doTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter, context, taskRunData.transaction, taskRunData)

        if (timeseriesStagingExceptionsExistForTaskRunPlotOrFilter) {
          context.log(`Ignoring message for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - Replay of failures is not supported yet`)
        } else if (timeseriesExistForTaskRunPlotOrFilter) {
          context.log(`Ignoring message for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - Timeseries have been imported`)
        } else {
          await importFromFews(context, taskRunData)
        }
      }
    } else {
      const errorMessage = `Unable to retrieve TIMESERIES_HEADER record for task run ${message.taskRunId}`
      await executePreparedStatementInTransaction(createStagingException, context, transaction, taskRunData, errorMessage)
    }
  } else {
    const errorMessage = 'Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes'
    await executePreparedStatementInTransaction(createStagingException, context, transaction, taskRunData, errorMessage)
  }
}

async function importFromFews (context, taskRunData) {
  try {
    await taskRunData['buildPiServerUrlIfPossibleFunction'](context, taskRunData)

    if (taskRunData.fewsPiUrl) {
      await retrieveFewsData(context, taskRunData)
      await executePreparedStatementInTransaction(loadFewsData, context, taskRunData.transaction, taskRunData)
    }
  } catch (err) {
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
          timeseriesHeaderId: taskRunData.timeseriesHeaderId,
          description: errorDescription
        }
      }
      await executePreparedStatementInTransaction(
        createTimeseriesStagingException,
        context,
        taskRunData.transaction,
        errorData
      )
    }
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
  taskRunData.fewsData = await gzip(fewsResponse.data)
  context.log(`Compressed data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
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

  const bindingDefinitions = JSON.stringify(context.bindingDefinitions)
  bindingDefinitions.includes(`"direction":"out"`) ? context.bindings.stagedTimeseries = [] : context.log(`No output binding attached.`)

  const parameters = {
    fewsData: taskRunData.fewsData,
    fewsParameters: taskRunData.fewsParameters,
    timeseriesHeaderId: taskRunData.timeseriesHeaderId
  }

  const result = await preparedStatement.execute(parameters)

  if (bindingDefinitions.includes(`"direction":"out"`)) {
    // Prepare to send a message containing the primary key of the inserted record.
    if (result.recordset && result.recordset[0] && result.recordset[0].id) {
      context.bindings.stagedTimeseries.push({
        id: result.recordset[0].id
      })
    }
  }

  context.log(`Loaded data for ${taskRunData.sourceTypeDescription} ID ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
}
