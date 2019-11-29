// const ImportTimeSeries = require('../Shared/timeseries-functions/importTimeSeries')
const ImportTimeSeriesDisplayGroups = require('../Shared/timeseries-functions/importTimeSeriesDisplayGroups')
const createStagingException = require('../Shared/create-staging-exception')
const { doInTransaction } = require('../Shared/transaction-helper')
const isTaskRunApproved = require('../Shared/is-task-run-approved')
const getWorkflowId = require('../Shared/get-workflowid')
const sql = require('mssql')

module.exports = async function (context, message) {
  // 'message' is the name of the variable that contains the queue item payload in the function code.

  async function routeMessage (transactionData) {
    context.log('JavaScript router ServiceBus queue trigger function processed message', message)
    const proceedWithImport = await isTaskRunApproved(context, message, transactionData.preparedStatement)
    if (proceedWithImport) {
      const workflowId = await getWorkflowId(context, message, transactionData.preparedStatement)
      const plotWorkflows = await getPlotWorkflows(context, new sql.Request(transactionData.transaction), workflowId)
      const filterWorkflows = await getFilterWorkslows(context, new sql.Request(transactionData.transaction), workflowId)
      await route(context, workflowId, plotWorkflows, filterWorkflows, message, transactionData.preparedStatement)
    } else {
      context.log.warn(`Ignoring message ${JSON.stringify(message)}`)
    }
  }
  await doInTransaction(routeMessage, context, 'The message routing function has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}

// get list of workflows associated with display groups (from ${FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA}.lookup_lookup)
async function getPlotWorkflows (context, request, workflowId) {
  const plotWorkflowQuery = await request.query(`
  BEGIN
      SELECT WORKFLOW_ID 
      FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.[LOCATION_LOOKUP]
      END
    `)
  const plotWorkflows = plotWorkflowQuery.recordset
  return plotWorkflows
}

// get list of display groups associated with timeseries (from £{FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA}.lookup_filter)
async function getFilterWorkslows (context, request, workflowId) {
  const filterWorkflowQuery = await request.query(`
  BEGIN
      SELECT WORKFLOW_ID 
      FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.[LOCATION_LOOKUP]
      END
    `) // Database name needs switching when database exists
  // const filterWorkflows = filterWorkflowQuery.recordset
  console.log(filterWorkflowQuery)
  const filterWorkflows = ['Lower_Derwent_to_Seaton_Mill_Forecast']
  return filterWorkflows
}

async function route (context, workflowId, plotWorkflows, filterWorkflows, message, preparedStatement) {
  if (plotWorkflows.indexOf(workflowId) >= 0) {
    context.log('plt function activated')
    await ImportTimeSeriesDisplayGroups(context, message, workflowId, preparedStatement)
  } else if (filterWorkflows.indexOf(workflowId) >= 0) {
    context.log('filter function activated')
    await ImportTimeSeriesDisplayGroups(context, message, workflowId, preparedStatement)
  } else {
    await createStagingException(context, message, `Missing location_lookup data for ${workflowId}`, preparedStatement)
  }
}
