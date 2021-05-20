const deactivateStagingExceptionBySourceFunctionAndTaskRunId = require('../../Shared/timeseries-functions/deactivate-staging-exceptions-by-source-function-and-task-run-id.js')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

const query = `
  select
    th.id
  from
    fff_staging.timeseries_header th
  where
    th.task_run_id = @taskRunId and
    not exists
    (
      select
        source_id,
        source_type
      from
        fff_staging.v_workflow w
      where
        w.workflow_id = th.workflow_id
      except
      (
        select
          source_id,
          source_type
        from
          fff_staging.timeseries t
        where
          t.timeseries_header_id = th.id
        union
        select
          source_id,
          source_type
        from
          fff_staging.v_active_timeseries_staging_exception tse
        where
          tse.timeseries_header_id = th.id
      )
    )
`
module.exports = async function (transaction, context, taskRunData) {
  // Staging exceptions created by the ImportFromFews function for a task run can be deactivated
  // if a timeseries or timeseries staging exception exists for every plot/filter of the associated workflow.
  if (await executePreparedStatementInTransaction(doTimeseriesOrTimeseriesStagingExceptionsExistForAllTaskRunPlotsAndFilters, context, transaction, taskRunData)) {
    taskRunData.transaction = transaction
    await deactivateStagingExceptionBySourceFunctionAndTaskRunId(context, taskRunData)
  }
}

async function doTimeseriesOrTimeseriesStagingExceptionsExistForAllTaskRunPlotsAndFilters (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.prepare(query)

  const parameters = {
    taskRunId: taskRunData.taskRunId
  }

  const result = await preparedStatement.execute(parameters)
  return Promise.resolve(!!(result.recordset && result.recordset[0]))
}
