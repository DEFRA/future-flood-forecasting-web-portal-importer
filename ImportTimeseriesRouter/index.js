const ImportTimeSeriesDisplayGroups = require('./timeseries-functions/importTimeSeriesDisplayGroups')
const ImportTimeSeries = require('./timeseries-functions/importTimeSeries')
const createStagingException = require('../Shared/create-staging-exception')
const { doInTransaction } = require('../Shared/transaction-helper')
const isTaskRunApproved = require('./helpers/is-task-run-approved')
const getWorkflowId = require('./helpers/get-workflowid')
const sql = require('mssql')

module.exports = async function (context, message) {
  // 'message' is the name of the variable that contains the queue item payload in the function code.
  // This function is triggered via a queue message drop
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
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}

// get list of workflows associated with display groups (from ${FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA}.lookup_lookup)
async function getfluvialDisplayGroupWorkflows (context, preparedStatement, workflowId) {
  await preparedStatement.input('workflowId', sql.NVarChar)

  // Run the query to retrieve location lookup data in a read only transaction with a table lock held
  // for the duration of the transaction to guard against a location lookup data refresh during
  // data retrieval.
  await preparedStatement.prepare(`
    select
      plot_id,
      location_ids
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup
    with
      (tablock holdlock)
    where
      workflow_id = @workflowId
  `)

  const parameters = {
    workflowId: workflowId
  }

  const fluvialDisplayGroupWorkflowsResponse = await preparedStatement.execute(parameters)

  // this statement has to be before another call to a funciton using a prepared statement
  if (preparedStatement && preparedStatement.prepared) {
    await preparedStatement.unprepare()
  }

  return fluvialDisplayGroupWorkflowsResponse
}

// get list of display groups associated with timeseries (from £{FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA}.lookup_filter)
async function getfluvialNonDisplayGroupWorkflows (context, preparedStatement, workflowId) {
  await preparedStatement.input('workflowId', sql.NVarChar)

  // Run the query to retrieve location lookup data in a read only transaction with a table lock held
  // for the duration of the transaction to guard against a location lookup data refresh during
  // data retrieval.
  await preparedStatement.prepare(`
    select
      filter_id
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW
    with
      (tablock holdlock)
    where
      workflow_id = @workflowId
  `)
  // TABLE DOESNT EXIST YET!!
  const parameters = {
    workflowId: workflowId
  }

  const fluvialNonDisplayGroupWorkflowsResponse = await preparedStatement.execute(parameters)

  // this statement has to be before another call to a funciton using a prepared statement
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
    await createStagingException(context, message, `Missing location_lookup data for ${workflowId}`, preparedStatement)
  }
}
