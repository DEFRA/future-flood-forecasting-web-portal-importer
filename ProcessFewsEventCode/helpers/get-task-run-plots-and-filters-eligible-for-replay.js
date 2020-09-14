const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

// Hold a table lock on the workflow view held for the duration of the transaction to guard
// against a workflow view refresh during processing.
const query = `
  select 
    source_id,
    source_type
  from
    fff_staging.v_workflow
  with
    (tablock holdlock)  
  where
    workflow_id = @workflowId
  intersect
  select
    tse.source_id,
    tse.source_type
  from
    fff_staging.timeseries_header th,
    fff_staging.v_active_timeseries_staging_exception tse
  where
    th.id = tse.timeseries_header_id and
    th.task_run_id = @taskRunId and
    (
      tse.csv_error = 0 or
      (
        tse.exception_time < (
          select
            wr.refresh_time
          from
            fff_staging.workflow_refresh wr
          where
            tse.csv_type = wr.csv_type
        )    
      )    
    )    
`
module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(getTaskRunPlotsAndFiltersEligibleForReplay, context, taskRunData.transaction, taskRunData)
}

async function getTaskRunPlotsAndFiltersEligibleForReplay (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(query)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  for (const record of result.recordset) {
    taskRunData.itemsEligibleForReplay.push({
      sourceId: record.source_id,
      sourceType: record.source_type
    })
  }
}