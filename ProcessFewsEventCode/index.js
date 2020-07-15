const moment = require('moment')
const createStagingException = require('../Shared/timeseries-functions/create-staging-exception')
const StagingError = require('../Shared/timeseries-functions/staging-error')
const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const doStagingExceptionsExistForTaskRun = require('./helpers/do-staging-exceptions-exist-for-task-run')
const doesTimeseriesHeaderExistForTaskRun = require('./helpers/does-timeseries-header-exist-for-task-run')
const doTimeseriesStagingExceptionsExistForTaskRun = require('./helpers/do-timeseries-staging-exceptions-exist-for-task-run')
const isForecast = require('./helpers/is-forecast')
const isIgnoredWorkflow = require('../Shared/timeseries-functions/is-ignored-workflow')
const isLatestTaskRunForWorkflow = require('../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const isTaskRunApproved = require('./helpers/is-task-run-approved')
const getTaskRunCompletionDate = require('./helpers/get-task-run-completion-date')
const getTaskRunStartDate = require('./helpers/get-task-run-start-date')
const getTaskRunId = require('./helpers/get-task-run-id')
const getWorkflowId = require('./helpers/get-workflow-id')
const preprocessMessage = require('./helpers/preprocess-message')
const sql = require('mssql')

module.exports = async function (context, message) {
  // This function is triggered via a queue message drop, 'message' is the name of the variable that contains the queue item payload.
  context.log.info('JavaScript import time series function processing work item', message)
  context.log.info(context.bindingData)
  await doInTransaction(processMessage, context, 'The message routing function has failed with the following error:', null, message)
  // context.done() not required in async functions
}

// Get a list of plots associated with the workflow.
async function getPlotsForWorkflow (context, preparedStatement, taskRunData) {
  if (taskRunData.forecast && !taskRunData.approved) {
    context.log.warn(`Ignoring unapproved forecast message ${JSON.stringify(taskRunData.message)}`)
  } else {
    await preparedStatement.input('displayGroupWorkflowId', sql.NVarChar)
    // Run the query within a transaction with a table lock held for the duration of the transaction to guard against a display group
    // data refresh during data retrieval.
    await preparedStatement.prepare(`
      select
        plot_id
      from
        fff_staging.${taskRunData.workflowTableName}
      with
        (tablock holdlock)
      where
        workflow_id = @displayGroupWorkflowId
   `)

    const parameters = {
      displayGroupWorkflowId: taskRunData.workflowId
    }

    const response = await preparedStatement.execute(parameters)
    return response
  }
}

// Get a list of filters associated with the workflow.
async function getFiltersForWorkflow (context, preparedStatement, taskRunData) {
  await preparedStatement.input('nonDisplayGroupWorkflowId', sql.NVarChar)

  // Run the query within a transaction with a table lock held for the duration of the transaction to guard
  // against a non display group data refresh during data retrieval.
  await preparedStatement.prepare(`
    select
      filter_id, approved, timeseries_type, start_time_offset_hours, end_time_offset_hours
    from
      fff_staging.non_display_group_workflow
    with
      (tablock holdlock)
    where
      workflow_id = @nonDisplayGroupWorkflowId
  `)
  const parameters = {
    nonDisplayGroupWorkflowId: taskRunData.workflowId
  }

  const response = await preparedStatement.execute(parameters)
  return response
}

async function createTimeseriesHeader (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunStartTime', sql.DateTime2)
  await preparedStatement.input('taskRunCompletionTime', sql.DateTime2)
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.input('forecast', sql.Bit)
  await preparedStatement.input('approved', sql.Bit)
  await preparedStatement.input('message', sql.NVarChar)

  await preparedStatement.prepare(`
  insert into
    fff_staging.timeseries_header
      (task_start_time, task_completion_time, task_run_id, workflow_id, forecast, approved, message)
  output
    inserted.id
  values
    (@taskRunStartTime, @taskRunCompletionTime, @taskRunId, @workflowId, @forecast, @approved, @message)
`)

  const parameters = {
    taskRunStartTime: taskRunData.taskRunStartTime,
    taskRunCompletionTime: taskRunData.taskRunCompletionTime,
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId,
    forecast: taskRunData.forecast,
    approved: taskRunData.approved,
    message: taskRunData.message
  }

  await preparedStatement.execute(parameters)
}

async function processTaskRunData (context, taskRunData, transaction) {
  const ignoredWorkflow =
    await executePreparedStatementInTransaction(isIgnoredWorkflow, context, taskRunData.transaction, taskRunData.workflowId)

  if (ignoredWorkflow) {
    context.log(`${taskRunData.workflowId} is an ignored workflow`)
  } else {
    const allDataRetrievalParameters = {
      fluvialDisplayGroupDataRetrievalParameters: {
        workflowsFunction: getPlotsForWorkflow,
        timeseriesDataFunctionType: 'plot',
        timeseriesDataIdentifier: 'plot_id',
        timeseriesDataMessageKey: 'plotId',
        workflowDataProperty: 'fluvialDisplayGroupWorkflowsResponse',
        workflowTableName: 'fluvial_display_group_workflow'
      },
      coastalDisplayGroupDataRetrievalParameters: {
        workflowsFunction: getPlotsForWorkflow,
        timeseriesDataFunctionType: 'plot',
        timeseriesDataIdentifier: 'plot_id',
        timeseriesDataMessageKey: 'plotId',
        workflowDataProperty: 'coastalDisplayGroupWorkflowsResponse',
        workflowTableName: 'coastal_display_group_workflow'
      },
      nonDisplayGroupDataRetrievalParameters: {
        workflowsFunction: getFiltersForWorkflow,
        timeseriesDataFunctionType: 'filter',
        timeseriesDataIdentifier: 'filter_id',
        timeseriesDataMessageKey: 'filterId',
        workflowDataProperty: 'nonDisplayGroupWorkflowsResponse',
        workflowTableName: 'non_display_group_workflow'
      }
    }

    let dataRetrievalParametersArray = []

    // Prepare to retrieve timeseries data for the workflow task run from the core engine PI server using workflow
    // reference data held in the staging database.
    if (taskRunData.forecast) {
      dataRetrievalParametersArray.push(allDataRetrievalParameters.fluvialDisplayGroupDataRetrievalParameters)
      dataRetrievalParametersArray.push(allDataRetrievalParameters.coastalDisplayGroupDataRetrievalParameters)
      // Core engine forecasts can be associated with display and non-display group CSV files.
      dataRetrievalParametersArray.push(allDataRetrievalParameters.nonDisplayGroupDataRetrievalParameters)
    } else {
      dataRetrievalParametersArray.push(allDataRetrievalParameters.nonDisplayGroupDataRetrievalParameters)
    }
    // Process data for each CSV file associated with the workflow.
    for (let dataRetrievalParameters of dataRetrievalParametersArray) {
      const timeseriesDataFunctionType = dataRetrievalParameters.timeseriesDataFunctionType
      const timeseriesDataIdentifier = dataRetrievalParameters.timeseriesDataIdentifier
      const timeseriesDataMessageKey = dataRetrievalParameters.timeseriesDataMessageKey
      const workflowDataProperty = dataRetrievalParameters.workflowDataProperty
      const workflowsFunction = dataRetrievalParameters.workflowsFunction

      // Retrieve workflow reference data for the current CSV file from the staging database.
      taskRunData.workflowTableName = dataRetrievalParameters.workflowTableName
      taskRunData[workflowDataProperty] = await executePreparedStatementInTransaction(workflowsFunction, context, taskRunData.transaction, taskRunData)

      if (taskRunData[workflowDataProperty] && taskRunData[workflowDataProperty].recordset) {
        // Create a message for each plot/filter associated with the current CSV file.
        for (const record of taskRunData[workflowDataProperty].recordset) {
          context.log(`Creating message for ${timeseriesDataFunctionType} ID ${record[timeseriesDataIdentifier]}`)
          const message = {
            taskRunId: taskRunData.taskRunId
          }
          message[timeseriesDataMessageKey] = record[timeseriesDataIdentifier]
          taskRunData.outgoingMessages.push(message)
        }
      }
    }

    if (taskRunData.outgoingMessages.length > 0) {
      // Create a timeseries header record and prepare to send a message for each plot/filter associated
      // with the task run.
      await executePreparedStatementInTransaction(
        createTimeseriesHeader,
        context,
        taskRunData.transaction,
        taskRunData)

      context.bindings.importFromFews = taskRunData.outgoingMessages
    } else {
      const errorMessage = `Missing PI Server input data for ${taskRunData.workflowId}`
      await executePreparedStatementInTransaction(
        createStagingException,
        context,
        taskRunData.transaction,
        taskRunData,
        errorMessage
      )
    }
  }
}

async function parseMessage (context, transaction, message) {
  const taskRunData = {
    message: message,
    transaction: transaction,
    throwStagingErrorFollowingStagingExceptionCreation: true,
    outgoingMessages: [],
    timeseriesStagingErrors: []
  }
  // Retrieve data from twelve hours before the task run completed to five days after the task run completed by default.
  // This time period can be overridden by the two environment variables
  // FEWS_START_TIME_OFFSET_HOURS and FEWS_END_TIME_OFFSET_HOURS.
  const startTimeDisplayGroupOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 14
  const endTimeDisplayGroupOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120

  taskRunData.taskRunId = await executePreparedStatementInTransaction(getTaskRunId, context, transaction, taskRunData)
  taskRunData.workflowId = await executePreparedStatementInTransaction(getWorkflowId, context, transaction, taskRunData)

  // The core engine uses UTC but does not appear to use ISO 8601 date formatting. As such dates need to be specified as
  // UTC using ISO 8601 date formatting manually to ensure portability between local and cloud environments.
  taskRunData.taskRunStartTime =
    moment(new Date(`${await executePreparedStatementInTransaction(getTaskRunStartDate, context, transaction, taskRunData)} UTC`)).toISOString()
  taskRunData.taskRunCompletionTime =
    moment(new Date(`${await executePreparedStatementInTransaction(getTaskRunCompletionDate, context, transaction, taskRunData)} UTC`)).toISOString()
  taskRunData.startTimeDisplayGroup = moment(taskRunData.taskRunCompletionTime).subtract(startTimeDisplayGroupOffsetHours, 'hours').toISOString()
  taskRunData.endTimeDisplayGroup = moment(taskRunData.taskRunCompletionTime).add(endTimeDisplayGroupOffsetHours, 'hours').toISOString()
  // Non display group times are calculated at load time
  taskRunData.forecast = await executePreparedStatementInTransaction(isForecast, context, transaction, taskRunData)
  taskRunData.approved = await executePreparedStatementInTransaction(isTaskRunApproved, context, transaction, taskRunData)
  return taskRunData
}

async function processMessage (transaction, context, message) {
  try {
    // If a JSON message is received convert it to a string.
    const preprocessedMessage = await executePreparedStatementInTransaction(preprocessMessage, context, transaction, message)
    if (preprocessedMessage) {
      const taskRunData = await parseMessage(context, transaction, preprocessedMessage)

      const timeseriesHeaderExistsForTaskRun =
        await executePreparedStatementInTransaction(doesTimeseriesHeaderExistForTaskRun, context, taskRunData.transaction, taskRunData)

      const stagingExceptionsExistForTaskRun =
        await executePreparedStatementInTransaction(doStagingExceptionsExistForTaskRun, context, taskRunData.transaction, taskRunData)

      const timeseriesStagingExceptionsExistForTaskRun =
        await executePreparedStatementInTransaction(doTimeseriesStagingExceptionsExistForTaskRun, context, taskRunData.transaction, taskRunData)

      if (stagingExceptionsExistForTaskRun || timeseriesStagingExceptionsExistForTaskRun) {
        context.log(`Ignoring message for task run ${taskRunData.taskRunId} - Replay of failures is not supported yet`)
      } else if (timeseriesHeaderExistsForTaskRun) {
        context.log(`Ignoring message for task run ${taskRunData.taskRunId} - Timeseries header has been created`)
      } else {
        // As the forecast and approved indicators are booleans progression must be based on them being defined.
        if (taskRunData.taskRunCompletionTime && taskRunData.workflowId && taskRunData.taskRunId &&
          typeof taskRunData.forecast !== 'undefined' && typeof taskRunData.approved !== 'undefined') {
          // Do not import out of date forecast data.
          if (!taskRunData.forecast || await executePreparedStatementInTransaction(isLatestTaskRunForWorkflow, context, transaction, taskRunData)) {
            await processTaskRunData(context, taskRunData, transaction)
          } else {
            context.log.warn(`Ignoring message for task run ${taskRunData.taskRunId} completed on ${taskRunData.taskRunCompletionTime}` +
              ` - ${taskRunData.latestTaskRunId} completed on ${taskRunData.latestTaskRunCompletionTime} is the latest task run for workflow ${taskRunData.workflowId}`)
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