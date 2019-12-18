const importTimeSeriesDisplayGroups = require('./timeseries-functions/importTimeSeriesDisplayGroups')
const importTimeSeries = require('./timeseries-functions/importTimeSeries')
const createStagingException = require('../Shared/create-staging-exception')
const { doInTransaction } = require('../Shared/transaction-helper')
const isTaskRunApproved = require('./helpers/is-task-run-approved')
const getTaskRunCompletionDate = require('./helpers/get-task-run-completion-date')
const getTaskRunId = require('./helpers/get-task-run-id')
const getWorkflowId = require('./helpers/get-workflow-id')
const sql = require('mssql')

module.exports = async function (context, message) {
  // This function is triggered via a queue message drop, 'message' is the name of the variable that contains the queue item payload
  context.log.info('JavaScript import time series function processed work item', message)
  context.log.info(context.bindingData)

  async function routeMessage (transactionData) {
    context.log('JavaScript router ServiceBus queue trigger function processed message', message)
    const proceedWithImport = await isTaskRunApproved(context, message, transactionData.preparedStatement)
    if (proceedWithImport) {
      const routeData = {
      }
      routeData.workflowId = await getWorkflowId(context, message, transactionData.preparedStatement)
      routeData.taskRunId = await getTaskRunId(context, message, transactionData.preparedStatement)
      routeData.taskRunCompletionDate = await getTaskRunCompletionDate(context, message, transactionData.preparedStatement)
      routeData.transactionData = transactionData

      routeData.fluvialDisplayGroupWorkflowsResponse =
        await getfluvialDisplayGroupWorkflows(context, transactionData.preparedStatement, routeData.workflowId)

      routeData.fluvialNonDisplayGroupWorkflowsResponse =
        await getfluvialNonDisplayGroupWorkflows(context, transactionData.preparedStatement, routeData.workflowId)

      await route(context, message, routeData)
    } else {
      context.log.warn(`Ignoring message ${JSON.stringify(message)}`)
    }
  }
  await doInTransaction(routeMessage, context, 'The message routing function has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  context.done()
}

// Get a list of workflows associated with display groups
async function getfluvialDisplayGroupWorkflows (context, preparedStatement, workflowId) {
  await preparedStatement.input('displayGroupWorkflowId', sql.NVarChar)

  // Run the query to retrieve display group data in a full transaction with a table lock held
  // for the duration of the transaction to guard against a display group data refresh during
  // data retrieval.
  await preparedStatement.prepare(`
    select
      plot_id,
      location_ids
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

  if (preparedStatement && preparedStatement.prepared) {
    await preparedStatement.unprepare()
  }

  return fluvialDisplayGroupWorkflowsResponse
}

// Get list of workflows associated with non display groups
async function getfluvialNonDisplayGroupWorkflows (context, preparedStatement, workflowId) {
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

  if (preparedStatement && preparedStatement.prepared) {
    await preparedStatement.unprepare()
  }

  return fluvialNonDisplayGroupWorkflowsResponse
}

async function route (context, message, routeData) {
  if (routeData.fluvialDisplayGroupWorkflowsResponse.recordset.length > 0) {
    context.log.info('Message routed to the plot function')
    await importTimeSeriesDisplayGroups(
      context,
      message,
      routeData.fluvialDisplayGroupWorkflowsResponse,
      routeData.workflowId,
      routeData.transactionData.preparedStatement
    )
  } else if (routeData.fluvialNonDisplayGroupWorkflowsResponse.recordset.length > 0) {
    context.log.info('Message has been routed to the filter function')
    await importTimeSeries(
      context,
      message,
      routeData.fluvialNonDisplayGroupWorkflowsResponse,
      routeData.workflowId,
      routeData.transactionData.preparedStatement
    )
  } else {
    await createStagingException(
      context,
      message,
      `Missing timeseries data for ${routeData.workflowId}`, routeData.transactionData.preparedStatement
    )
  }
}
