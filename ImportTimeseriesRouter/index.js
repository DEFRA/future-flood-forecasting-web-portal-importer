const moment = require('moment')
const getTimeSeriesDisplayGroups = require('./timeseries-functions/importTimeSeriesDisplayGroups')
const getTimeSeriesNonDisplayGroups = require('./timeseries-functions/importTimeSeries')
const createStagingException = require('../Shared/create-staging-exception')
const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const isTaskRunApproved = require('./helpers/is-task-run-approved')
const getTaskRunCompletionDate = require('./helpers/get-task-run-completion-date')
const getTaskRunId = require('./helpers/get-task-run-id')
const getWorkflowId = require('./helpers/get-workflow-id')
const sql = require('mssql')

module.exports = async function (context, message) {
  // This function is triggered via a queue message drop, 'message' is the name of the variable that contains the queue item payload
  context.log.info('JavaScript import time series function processed work item', message)
  context.log.info(context.bindingData)

  async function routeMessage (transaction, context) {
    context.log('JavaScript router ServiceBus queue trigger function processed message', message)
    const proceedWithImport = await executePreparedStatementInTransaction(isTaskRunApproved, context, transaction, message)
    if (proceedWithImport) {
      const routeData = {
      }
      // Retrieve data from two days before the task run completed to five days after the task run completed by default.
      // This time period can be overridden by the two environment variables
      // FEWS_START_TIME_OFFSET_HOURS and FEWS_END_TIME_OFFSET_HOURS.
      const startTimeOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 48
      const endTimeOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120
      routeData.taskCompletionTime = await executePreparedStatementInTransaction(getTaskRunCompletionDate, context, transaction, message)
      routeData.startTime = moment(routeData.taskCompletionTime).subtract(startTimeOffsetHours, 'hours').toISOString()
      routeData.endTime = moment(routeData.taskCompletionTime).add(endTimeOffsetHours, 'hours').toISOString()
      routeData.workflowId = await executePreparedStatementInTransaction(getWorkflowId, context, transaction, message)
      routeData.taskId = await executePreparedStatementInTransaction(getTaskRunId, context, transaction, message)
      routeData.transaction = transaction

      routeData.fluvialDisplayGroupWorkflowsResponse =
        await executePreparedStatementInTransaction(getFluvialDisplayGroupWorkflows, context, transaction, routeData.workflowId)

      routeData.fluvialNonDisplayGroupWorkflowsResponse =
        await executePreparedStatementInTransaction(getFluvialNonDisplayGroupWorkflows, context, transaction, routeData.workflowId)

      routeData.ignoredWorkflowsResponse =
        await executePreparedStatementInTransaction(getIgnoredWorkflows, context, transaction, routeData.workflowId)

      await route(context, message, routeData)
    } else {
      context.log.warn(`Ignoring message ${JSON.stringify(message)}`)
    }
  }
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
}

async function route (context, message, routeData) {
  if (routeData.ignoredWorkflowsResponse.recordset.length === 0) {
    if (routeData.fluvialDisplayGroupWorkflowsResponse.recordset.length > 0 ||
      routeData.fluvialNonDisplayGroupWorkflowsResponse.recordset.length > 0) {
      let timeseriesData
      routeData.timeseriesHeaderId = await executePreparedStatementInTransaction(
        createTimeseriesHeader,
        context,
        routeData.transaction,
        message,
        routeData
      )

      if (routeData.fluvialDisplayGroupWorkflowsResponse.recordset.length > 0) {
        context.log.info('Message routed to the plot function')
        timeseriesData = await getTimeSeriesDisplayGroups(context, routeData)
      } else if (routeData.fluvialNonDisplayGroupWorkflowsResponse.recordset.length > 0) {
        context.log.info('Message has been routed to the filter function')
        timeseriesData = await getTimeSeriesNonDisplayGroups(context, routeData)
      }
      await executePreparedStatementInTransaction(
        loadTimeseries,
        context,
        routeData.transaction,
        timeseriesData,
        routeData
      )
    } else {
      const errorMessage =
       routeData.workflowId ? `Missing PI Server input data for ${routeData.workflowId}`
         : 'Unable to determine PI Server input data for unknown workflow'

      await executePreparedStatementInTransaction(
        createStagingException,
        context,
        routeData.transaction,
        message,
        errorMessage
      )
    }
  } else {
    context.log(`${routeData.workflowId} is an ignored workflow`)
  }
}
