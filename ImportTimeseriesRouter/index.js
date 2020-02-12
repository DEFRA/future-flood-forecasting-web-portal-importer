const moment = require('moment')
const getTimeSeriesDisplayGroups = require('./timeseries-functions/importTimeSeriesDisplayGroups')
const getTimeSeriesNonDisplayGroups = require('./timeseries-functions/importTimeSeries')
const createStagingException = require('../Shared/create-staging-exception')
const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const isForecast = require('./helpers/is-forecast')
const isTaskRunApproved = require('./helpers/is-task-run-approved')
const getTaskRunCompletionDate = require('./helpers/get-task-run-completion-date')
const getTaskRunId = require('./helpers/get-task-run-id')
const getWorkflowId = require('./helpers/get-workflow-id')
const preprocessMessage = require('./helpers/preprocess-message')
const sql = require('mssql')

module.exports = async function (context, message) {
  async function routeMessage (transaction, context) {
    const routeData = {
    }

    // If a JSON message is received convert it to a string.
    const preprocessedMessage = await executePreparedStatementInTransaction(preprocessMessage, context, transaction, message, true)

    if (preprocessedMessage) {
      // Retrieve data from twelve hours before the task run completed to five days after the task run completed by default.
      // This time period can be overridden by the two environment variables
      // FEWS_START_TIME_OFFSET_HOURS and FEWS_END_TIME_OFFSET_HOURS.
      const startTimeOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 12
      const endTimeOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120
      routeData.taskCompletionTime = await executePreparedStatementInTransaction(getTaskRunCompletionDate, context, transaction, preprocessedMessage)
      routeData.startTime = moment(routeData.taskCompletionTime).subtract(startTimeOffsetHours, 'hours').toISOString()
      routeData.endTime = moment(routeData.taskCompletionTime).add(endTimeOffsetHours, 'hours').toISOString()
      routeData.workflowId = await executePreparedStatementInTransaction(getWorkflowId, context, transaction, preprocessedMessage)
      routeData.taskId = await executePreparedStatementInTransaction(getTaskRunId, context, transaction, preprocessedMessage)
      routeData.forecast = await executePreparedStatementInTransaction(isForecast, context, transaction, preprocessedMessage)
      routeData.approved = await executePreparedStatementInTransaction(isTaskRunApproved, context, transaction, preprocessedMessage)
      routeData.transaction = transaction

      // As the forecast and approved indicators are booleans progression must be based on them being defined.
      if (routeData.taskCompletionTime && routeData.workflowId && routeData.taskId &&
        typeof routeData.forecast !== 'undefined' && typeof routeData.approved !== 'undefined') {
        await route(context, preprocessedMessage, routeData)
      }
    }
  }

  // This function is triggered via a queue message drop, 'message' is the name of the variable that contains the queue item payload
  context.log.info('JavaScript import time series function processing work item', message)
  context.log.info(context.bindingData)
  await doInTransaction(routeMessage, context, 'The message routing function has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  context.done()
}

// Get a list of workflows associated with display groups
async function getFluvialDisplayGroupWorkflows (context, preparedStatement, workflowId) {
  await preparedStatement.input('displayGroupWorkflowId', sql.NVarChar)

  // Run the query to retrieve display group data in a full transaction with a table lock held
  // for the duration of the transaction to guard against a display group data refresh during
  // data retrieval.
  await preparedStatement.prepare(`
  select
    plot_id, location_ids
  from
    ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FLUVIAL_DISPLAY_GROUP_WORKFLOW
  with
    (tablock holdlock)
  where
    workflow_id = @displayGroupWorkflowId
`)

  const parameters = {
    displayGroupWorkflowId: workflowId
  }

  const fluvialDisplayGroupWorkflowsResponse = await preparedStatement.execute(parameters)
  return fluvialDisplayGroupWorkflowsResponse
}

// Get list of workflows associated with non display groups
async function getFluvialNonDisplayGroupWorkflows (context, preparedStatement, workflowId) {
  await preparedStatement.input('nonDisplayGroupWorkflowId', sql.NVarChar)

  // Run the query to retrieve non display group data in a full transaction with a table lock held
  // for the duration of the transaction to guard against a non display group data refresh during
  // data retrieval.
  await preparedStatement.prepare(`
  select
    filter_id
  from
    ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW
  with
    (tablock holdlock)
  where
    workflow_id = @nonDisplayGroupWorkflowId
`)
  const parameters = {
    nonDisplayGroupWorkflowId: workflowId
  }

  const fluvialNonDisplayGroupWorkflowsResponse = await preparedStatement.execute(parameters)
  return fluvialNonDisplayGroupWorkflowsResponse
}

// Get list of ignored workflows
async function getIgnoredWorkflows (context, preparedStatement, workflowId) {
  await preparedStatement.input('workflowId', sql.NVarChar)

  // Run the query to retrieve ignored workflow data in a full transaction with a table lock held
  // for the duration of the transaction to guard against an ignored workflow data refresh during
  // data retrieval.
  await preparedStatement.prepare(`
  select
    workflow_id
  from
    ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.IGNORED_WORKFLOW
  with
    (tablock holdlock)
  where
    workflow_id = @workflowId
`)
  const parameters = {
    workflowId
  }

  const ignoredWorkflowsResponse = await preparedStatement.execute(parameters)
  return ignoredWorkflowsResponse
}

async function createTimeseriesHeader (context, preparedStatement, message, routeData) {
  let timeseriesHeaderId

  await preparedStatement.input('startTime', sql.DateTime2)
  await preparedStatement.input('endTime', sql.DateTime2)
  await preparedStatement.input('taskCompletionTime', sql.DateTime2)
  await preparedStatement.input('taskId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.output('insertedId', sql.UniqueIdentifier)

  await preparedStatement.prepare(`
  insert into
    ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header (start_time, end_time, task_completion_time, task_id, workflow_id)
  output
    inserted.id
  values
    (@startTime, @endTime, @taskCompletionTime, @taskId, @workflowId)
`)

  const parameters = {
    startTime: routeData.startTime,
    endTime: routeData.endTime,
    taskCompletionTime: routeData.taskCompletionTime,
    taskId: routeData.taskId,
    workflowId: routeData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  // Return the primary key of the new TIMESERIES_HEADER record so that
  // new TIMESERIES records can link to it.
  if (result.recordset && result.recordset[0] && result.recordset[0].id) {
    timeseriesHeaderId = result.recordset[0].id
  }
  return timeseriesHeaderId
}

async function loadTimeseries (context, preparedStatement, timeSeriesData, routeData) {
  context.log('Loading timeseries data')
  await preparedStatement.input('fewsData', sql.NVarChar)
  await preparedStatement.input('fewsParameters', sql.NVarChar)
  await preparedStatement.input('timeseriesHeaderId', sql.NVarChar)
  await preparedStatement.output('insertedId', sql.UniqueIdentifier)

  await preparedStatement.prepare(`
  insert into
    ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries (fews_data, fews_parameters, timeseries_header_id)
  output
    inserted.id
  values
    (@fewsData, @fewsParameters, @timeseriesHeaderId)
`)

  context.bindings.stagedTimeseries = []

  for (const index in timeSeriesData) {
    const parameters = {
      fewsData: timeSeriesData[index].fewsData,
      fewsParameters: timeSeriesData[index].fewsParameters,
      timeseriesHeaderId: routeData.timeseriesHeaderId
    }

    const result = await preparedStatement.execute(parameters)

    // Prepare to send a message containing the primary key of the inserted record.
    if (result.recordset && result.recordset[0] && result.recordset[0].id) {
      context.bindings.stagedTimeseries.push({
        id: result.recordset[0].id
      })
    }
  }
  context.log('Loaded timeseries data')
}

async function route (context, message, routeData) {
  const ignoredWorkflowsResponse =
    await executePreparedStatementInTransaction(getIgnoredWorkflows, context, routeData.transaction, routeData.workflowId)

  const ignoredWorkflow = ignoredWorkflowsResponse.recordset.length > 0

  if (ignoredWorkflow) {
    context.log(`${routeData.workflowId} is an ignored workflow`)
  } else if (routeData.forecast && !routeData.approved) {
    context.log.warn(`Ignoring unapproved forecast message ${JSON.stringify(message)}`)
  } else {
    // Import data for approved task runs of display group workflows and all tasks runs of non-display group workflows.
    let timeseriesData
    let timeseriesDataFunction
    let timeseriesDataFunctionType
    let workflowDataProperty
    let workflowsFunction

    routeData.timeseriesHeaderId = await executePreparedStatementInTransaction(
      createTimeseriesHeader,
      context,
      routeData.transaction,
      message,
      routeData
    )

    // Prepare to retrieve timeseries data for the workflow task run from the core engine PI server using workflow
    // reference data held in the staging database.
    if (routeData.forecast) {
      workflowsFunction = getFluvialDisplayGroupWorkflows
      timeseriesDataFunction = getTimeSeriesDisplayGroups
      timeseriesDataFunctionType = 'plot'
      workflowDataProperty = 'fluvialDisplayGroupWorkflowsResponse'
    } else {
      workflowsFunction = getFluvialNonDisplayGroupWorkflows
      timeseriesDataFunction = getTimeSeriesNonDisplayGroups
      timeseriesDataFunctionType = 'filter'
      workflowDataProperty = 'fluvialNonDisplayGroupWorkflowsResponse'
    }

    // Retrieve workflow reference data from the staging database.
    routeData[workflowDataProperty] = await executePreparedStatementInTransaction(workflowsFunction, context, routeData.transaction, routeData.workflowId)

    if (routeData[workflowDataProperty].recordset.length > 0) {
      context.log.info(`Message has been routed to the ${timeseriesDataFunctionType} function`)
      // Retrieve timeseries data from the core engine PI server and load it into the staging database.
      timeseriesData = await timeseriesDataFunction(context, routeData)
      await executePreparedStatementInTransaction(
        loadTimeseries,
        context,
        routeData.transaction,
        timeseriesData,
        routeData
      )
    } else {
      const errorMessage = `Missing PI Server input data for ${routeData.workflowId}`

      await executePreparedStatementInTransaction(
        createStagingException,
        context,
        routeData.transaction,
        message,
        errorMessage
      )
    }
  }
}
