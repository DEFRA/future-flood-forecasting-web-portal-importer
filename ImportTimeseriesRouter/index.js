const moment = require('moment')
const getTimeSeriesDisplayGroups = require('./timeseries-functions/importTimeSeriesDisplayGroups')
const getTimeSeriesNonDisplayGroups = require('./timeseries-functions/importTimeSeries')
const createStagingException = require('../Shared/create-staging-exception')
const StagingError = require('../Shared/staging-error')
const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const isForecast = require('./helpers/is-forecast')
const isLatestTaskRunForWorkflow = require('./helpers/is-latest-task-run-for-workflow')
const isTaskRunApproved = require('./helpers/is-task-run-approved')
const isTaskRunImported = require('./helpers/is-task-run-imported')
const getTaskRunCompletionDate = require('./helpers/get-task-run-completion-date')
const getTaskRunId = require('./helpers/get-task-run-id')
const getWorkflowId = require('./helpers/get-workflow-id')
const preprocessMessage = require('./helpers/preprocess-message')
const sql = require('mssql')

module.exports = async function (context, message) {
  // This function is triggered via a queue message drop, 'message' is the name of the variable that contains the queue item payload.
  context.log.info('JavaScript import time series function processing work item', message)
  context.log.info(context.bindingData)
  await doInTransaction(routeMessage, context, 'The message routing function has failed with the following error:', null, message)
  context.done()
}

// Get a list of workflows associated with display groups
async function getDisplayGroupWorkflows (context, preparedStatement, routeData) {
  if (routeData.forecast && !routeData.approved) {
    context.log.warn(`Ignoring unapproved forecast message ${JSON.stringify(routeData.message)}`)
  } else {
    await preparedStatement.input('displayGroupWorkflowId', sql.NVarChar)
    // Run the query to retrieve display group data in a full transaction with a table lock held
    // for the duration of the transaction to guard against a display group data refresh during
    // data retrieval.
    await preparedStatement.prepare(`
      select
        plot_id, location_ids
      from
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${routeData.workflowTableName}
      with
        (tablock holdlock)
      where
        workflow_id = @displayGroupWorkflowId
   `)

    const parameters = {
      displayGroupWorkflowId: routeData.workflowId
    }

    const fluvialDisplayGroupWorkflowsResponse = await preparedStatement.execute(parameters)
    return fluvialDisplayGroupWorkflowsResponse
  }
}

// Get list of workflows associated with non display groups
async function getNonDisplayGroupWorkflows (context, preparedStatement, routeData) {
  await preparedStatement.input('nonDisplayGroupWorkflowId', sql.NVarChar)

  // Ensure the query to retrieve non display group data takes account of core engine forecasts and external forecasts.
  // If the core engine message is associated with a forecast, search for matching workflows where the forecast
  // attribute is set. If the core engine message is not associated with a forecast, search for all matching
  // workflows so that external forecast data is retrieved.
  // Run the query in a full transaction with a table lock held for the duration of the transaction to guard
  // against a non display group data refresh during data retrieval.
  await preparedStatement.prepare(`
    select
      filter_id
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow
    with
      (tablock holdlock)
    where
      workflow_id = @nonDisplayGroupWorkflowId
      ${routeData.forecast ? ' and forecast = 1' : ''}
  `)
  const parameters = {
    nonDisplayGroupWorkflowId: routeData.workflowId
  }

  const nonDisplayGroupWorkflowsResponse = await preparedStatement.execute(parameters)
  return nonDisplayGroupWorkflowsResponse
}

// Get list of ignored workflows.
async function getIgnoredWorkflows (context, preparedStatement, workflowId) {
  await preparedStatement.input('workflowId', sql.NVarChar)

  // Run the query to retrieve ignored workflow data in a full transaction with a table lock held
  // for the duration of the transaction to guard against an ignored workflow data refresh during
  // data retrieval.
  await preparedStatement.prepare(`
  select
    workflow_id
  from
    ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow
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

async function createTimeseriesHeader (context, preparedStatement, routeData) {
  let timeseriesHeaderId

  await preparedStatement.input('startTime', sql.DateTime2)
  await preparedStatement.input('endTime', sql.DateTime2)
  await preparedStatement.input('taskCompletionTime', sql.DateTime2)
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.output('insertedId', sql.UniqueIdentifier)

  await preparedStatement.prepare(`
  insert into
    ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header (start_time, end_time, task_completion_time, task_run_id, workflow_id)
  output
    inserted.id
  values
    (@startTime, @endTime, @taskCompletionTime, @taskRunId, @workflowId)
`)

  const parameters = {
    startTime: routeData.startTime,
    endTime: routeData.endTime,
    taskCompletionTime: routeData.taskCompletionTime,
    taskRunId: routeData.taskRunId,
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

async function route (context, routeData) {
  const ignoredWorkflowsResponse =
    await executePreparedStatementInTransaction(getIgnoredWorkflows, context, routeData.transaction, routeData.workflowId)

  const ignoredWorkflow = ignoredWorkflowsResponse.recordset.length > 0

  if (ignoredWorkflow) {
    context.log(`${routeData.workflowId} is an ignored workflow`)
  } else {
    // Import data for approved task runs of display group workflows and all tasks runs of non-display group workflows.
    const allDataRetrievalParameters = {
      fluvialDisplayGroupDataRetrievalParameters: {
        workflowsFunction: getDisplayGroupWorkflows,
        timeseriesDataFunction: getTimeSeriesDisplayGroups,
        timeseriesDataFunctionType: 'plot',
        workflowDataProperty: 'fluvialDisplayGroupWorkflowsResponse',
        workflowTableName: 'fluvial_display_group_workflow'
      },
      nonDisplayGroupDataRetrievalParameters: {
        workflowsFunction: getNonDisplayGroupWorkflows,
        timeseriesDataFunction: getTimeSeriesNonDisplayGroups,
        timeseriesDataFunctionType: 'filter',
        workflowDataProperty: 'nonDisplayGroupWorkflowsResponse'
      }
    }

    let dataRetrievalParametersArray = []

    // Prepare to retrieve timeseries data for the workflow task run from the core engine PI server using workflow
    // reference data held in the staging database.
    if (routeData.forecast) {
      dataRetrievalParametersArray.push(allDataRetrievalParameters.fluvialDisplayGroupDataRetrievalParameters)
      // Core engine forecasts can be associated with display and non-display group CSV files.
      dataRetrievalParametersArray.push(allDataRetrievalParameters.nonDisplayGroupDataRetrievalParameters)
    } else {
      dataRetrievalParametersArray.push(allDataRetrievalParameters.nonDisplayGroupDataRetrievalParameters)
    }

    for (let dataRetrievalParameters of dataRetrievalParametersArray) {
      let timeseriesData
      const timeseriesDataFunction = dataRetrievalParameters.timeseriesDataFunction
      const timeseriesDataFunctionType = dataRetrievalParameters.timeseriesDataFunctionType
      const workflowDataProperty = dataRetrievalParameters.workflowDataProperty
      const workflowsFunction = dataRetrievalParameters.workflowsFunction

      // Retrieve workflow reference data from the staging database.
      if (dataRetrievalParameters.workflowTableName) {
        routeData.workflowTableName = dataRetrievalParameters.workflowTableName
      }

      routeData[workflowDataProperty] = await executePreparedStatementInTransaction(workflowsFunction, context, routeData.transaction, routeData)

      if (routeData[workflowDataProperty] && routeData[workflowDataProperty].recordset.length > 0) {
        context.log.info(`Message has been routed to the ${timeseriesDataFunctionType} function`)

        if (!routeData.timeseriesHeaderId) {
          routeData.timeseriesHeaderId = await executePreparedStatementInTransaction(
            createTimeseriesHeader,
            context,
            routeData.transaction,
            routeData
          )
        }

        // Retrieve timeseries data from the core engine PI server and load it into the staging database.
        timeseriesData = await timeseriesDataFunction(context, routeData)
        await executePreparedStatementInTransaction(
          loadTimeseries,
          context,
          routeData.transaction,
          timeseriesData,
          routeData
        )
      }
    }

    if (!routeData.timeseriesHeaderId) {
      const errorMessage = `Missing PI Server input data for ${routeData.workflowId}`

      await executePreparedStatementInTransaction(
        createStagingException,
        context,
        routeData.transaction,
        routeData.message,
        errorMessage
      )
    }
  }
}

async function parseMessage (context, transaction, message) {
  const routeData = {
    message: message,
    transaction: transaction
  }
  // Retrieve data from twelve hours before the task run completed to five days after the task run completed by default.
  // This time period can be overridden by the two environment variables
  // FEWS_START_TIME_OFFSET_HOURS and FEWS_END_TIME_OFFSET_HOURS.
  const startTimeOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 12
  const endTimeOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120

  // The core engine uses UTC but does not appear to use ISO 8601 date formatting. As such dates need to be specified as
  // UTC using ISO 8601 date formatting manually to ensure portability between local and cloud environments.
  routeData.taskCompletionTime =
    moment(new Date(`${await executePreparedStatementInTransaction(getTaskRunCompletionDate, context, transaction, message)} UTC`)).toISOString()

  routeData.startTime = moment(routeData.taskCompletionTime).subtract(startTimeOffsetHours, 'hours').toISOString()
  routeData.endTime = moment(routeData.taskCompletionTime).add(endTimeOffsetHours, 'hours').toISOString()
  routeData.workflowId = await executePreparedStatementInTransaction(getWorkflowId, context, transaction, message)
  routeData.taskRunId = await executePreparedStatementInTransaction(getTaskRunId, context, transaction, message)
  routeData.forecast = await executePreparedStatementInTransaction(isForecast, context, transaction, message)
  routeData.approved = await executePreparedStatementInTransaction(isTaskRunApproved, context, transaction, message)
  return routeData
}

async function routeMessage (transaction, context, message) {
  try {
    // If a JSON message is received convert it to a string.
    const preprocessedMessage = await executePreparedStatementInTransaction(preprocessMessage, context, transaction, message)
    if (preprocessedMessage) {
      const routeData = await parseMessage(context, transaction, preprocessedMessage)
      if (await executePreparedStatementInTransaction(isTaskRunImported, context, transaction, routeData.taskRunId)) {
        context.log.warn(`Ignoring message for task run ${routeData.taskRunId} - data has been imported already`)
      } else {
        // As the forecast and approved indicators are booleans progression must be based on them being defined.
        if (routeData.taskCompletionTime && routeData.workflowId && routeData.taskRunId &&
          typeof routeData.forecast !== 'undefined' && typeof routeData.approved !== 'undefined') {
          // Do not import out of date forecast data.
          if (!routeData.forecast || await executePreparedStatementInTransaction(isLatestTaskRunForWorkflow, context, transaction, routeData)) {
            await route(context, routeData)
          } else {
            context.log.warn(`Ignoring message for task run ${routeData.taskRunId} completed on ${routeData.taskCompletionTime}` +
              ` - ${routeData.latestTaskRunId} completed on ${routeData.latestTaskCompletionTime} is the latest task run for workflow ${routeData.workflowId}`)
          }
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
