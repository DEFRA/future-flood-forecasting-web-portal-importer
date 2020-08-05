const sql = require('mssql')
const { executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const getUnprocessedTaskRunPlotsAndFilters = require('./get-unprocessed-task-run-plots-and-filters')
const getTaskRunPlotsAndFiltersEligibleForReplay = require('./get-task-run-plots-and-filters-eligible-for-replay')

module.exports = async function (context, taskRunData) {
  if (taskRunData.timeseriesHeaderExistForTaskRun) {
    await executePreparedStatementInTransaction(getUnprocessedTaskRunPlotsAndFilters, context, taskRunData.transaction, taskRunData)
    await executePreparedStatementInTransaction(getTaskRunPlotsAndFiltersEligibleForReplay, context, taskRunData.transaction, taskRunData)
  } else {
    await executePreparedStatementInTransaction(getAllPlotsAndFiltersForWorkflow, context, taskRunData.transaction, taskRunData)
  }
}
async function getAllPlotsAndFiltersForWorkflow (context, preparedStatement, taskRunData) {
  await preparedStatement.input('nonDisplayGroupWorkflowId', sql.NVarChar)

  // Hold a table lock on the workflow view held for the duration of the transaction to guard
  // against a workflow view refresh during processing.
  await preparedStatement.prepare(`
    select
      source_id,
      source_type
    from
      fff_staging.v_workflow
    with
      (tablock holdlock)
    where
      workflow_id = @workflowId
  `)
  const parameters = {
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  for (const record of result.recordset) {
    taskRunData.unprocessedItems.push({
      sourceId: record.source_id,
      sourceType: record.source_type
    })
  }
}
