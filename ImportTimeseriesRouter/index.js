const ImportTimeSeriesDisplayGroups = require('./timeseries-functions/importTimeSeriesDisplayGroups')
const ImportTimeSeries = require('./timeseries-functions/importTimeSeries')
const createStagingException = require('../Shared/create-staging-exception')
const { doInTransaction } = require('../Shared/transaction-helper')
const isTaskRunApproved = require('./helpers/is-task-run-approved')
const getWorkflowId = require('./helpers/get-workflowid')
const sql = require('mssql')

module.exports = async function (context, message) {
  // This function is triggered via a queue message drop, 'message' is the name of the variable that contains the queue item payload
  context.log.info('JavaScript import time series function processed work item', message)
  context.log.info(context.bindingData)

  async function routeMessage (transactionData) {
    context.log('JavaScript router ServiceBus queue trigger function processed message', message)
    const proceedWithImport = await isTaskRunApproved(context, message, transactionData.preparedStatement)
    if (proceedWithImport) {
      const workflowId = await getWorkflowId(context, message, transactionData.preparedStatement)
      const fluvialDisplayGroupWorkflowsResponse = await getfluvialDisplayGroupWorkflows(context, transactionData.preparedStatement, workflowId)
      const fluvialNonDisplayGroupWorkflowsResponse = await getfluvialNonDisplayGroupWorkflows(context, transactionData.preparedStatement, workflowId)
      await route(context, workflowId, fluvialDisplayGroupWorkflowsResponse, fluvialNonDisplayGroupWorkflowsResponse, message, transactionData.preparedStatement)
    } else {
      context.log.warn(`Ignoring message ${JSON.stringify(message)}`)
    }
  }
  await doInTransaction(routeMessage, context, 'The message routing function has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() is not requried as the async function returns the desired result, there is no output binding to be activated.
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

async function route (context, workflowId, fluvialDisplayGroupWorkflowsResponse, fluvialNonDisplayGroupWorkflowsResponse, message, preparedStatement) {
  if (fluvialDisplayGroupWorkflowsResponse.recordset.length > 0) {
    context.log.info('Message routed to the plot function')
    await ImportTimeSeriesDisplayGroups(context, message, fluvialDisplayGroupWorkflowsResponse, workflowId, preparedStatement)
  } else if (fluvialNonDisplayGroupWorkflowsResponse.recordset.length > 0) {
    context.log.info('Message has been routed to the filter function')
    await ImportTimeSeries(context, message, fluvialNonDisplayGroupWorkflowsResponse, workflowId, preparedStatement)
  } else {
    await createStagingException(context, message, `Missing timeseries data for ${workflowId}`, preparedStatement)
  }
}
