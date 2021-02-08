const sql = require('mssql')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const getUnprocessedTaskRunPlotsAndFilters = require('./get-unprocessed-task-run-plots-and-filters')
const getTaskRunPlotsAndFiltersEligibleForReplay = require('./get-task-run-plots-and-filters-eligible-for-replay')

// Note that table locks are held on each table used by the workflow view for the duration of the transaction to
// guard against a workflow table refresh during processing.
const query = `
  select distinct
    source_id,
    source_type
  from
    fff_staging.v_workflow
  where
    workflow_id = @workflowId
`

module.exports = async function (context, taskRunData) {
  if (taskRunData.timeseriesHeaderExistsForTaskRun) {
    await getUnprocessedTaskRunPlotsAndFilters(context, taskRunData)
    await getTaskRunPlotsAndFiltersEligibleForReplay(context, taskRunData)
  } else {
    await executePreparedStatementInTransaction(getAllPlotsAndFiltersForWorkflow, context, taskRunData.transaction, taskRunData)
  }
  taskRunData.itemsToBeProcessed = taskRunData.unprocessedItems.concat(taskRunData.itemsEligibleForReplay)
}

async function getAllPlotsAndFiltersForWorkflow (context, preparedStatement, taskRunData) {
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(query)

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
