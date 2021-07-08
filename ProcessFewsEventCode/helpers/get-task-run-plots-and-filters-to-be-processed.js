import sql from 'mssql'
import { executePreparedStatementInTransaction } from '../../Shared/transaction-helper.js'
import getItemsToBeProcessedAsArray from './get-items-to-be-processed-as-array.js'
import getUnprocessedTaskRunPlotsAndFilters from './get-unprocessed-task-run-plots-and-filters.js'
import getTaskRunPlotsAndFiltersEligibleForReplay from './get-task-run-plots-and-filters-eligible-for-replay.js'

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

export default async function (context, taskRunData) {
  if (taskRunData.timeseriesHeaderExistsForTaskRun) {
    await getUnprocessedTaskRunPlotsAndFilters(context, taskRunData)
    await getTaskRunPlotsAndFiltersEligibleForReplay(context, taskRunData)
  } else {
    await executePreparedStatementInTransaction(getAllPlotsAndFiltersForWorkflow, context, taskRunData.transaction, taskRunData)
  }
  taskRunData.itemsToBeProcessed = taskRunData.unprocessedItems.concat(taskRunData.itemsEligibleForReplay)
  return Promise.resolve(taskRunData)
}

async function getAllPlotsAndFiltersForWorkflow (context, preparedStatement, taskRunData) {
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(query)

  const parameters = {
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)
  const unprocessedItems = await getItemsToBeProcessedAsArray(result.recordset)
  taskRunData.unprocessedItems.push(...unprocessedItems)
}
