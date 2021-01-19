const deactivateStagingExceptionBySourceFunctionAndTaskRunId = require('../../Shared/timeseries-functions/deactivate-staging-exceptions-by-source-function-and-task-run-id.js')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  // Staging exceptions created by the ImportFromFews function for a task run can be deactivated
  // if a timeseries or timeseries staging exception exists for every plot/filter of the associated workflow.
  if (await executePreparedStatementInTransaction(doTimeseriesOrTimeseriesStagingExceptionsExistForAllTaskRunPlotsAndFilters, context, taskRunData.transaction, taskRunData)) {
    await deactivateStagingExceptionBySourceFunctionAndTaskRunId(context, taskRunData)
  }
}

async function doTimeseriesOrTimeseriesStagingExceptionsExistForAllTaskRunPlotsAndFilters (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('sourceId', sql.NVarChar)
  await preparedStatement.input('sourceType', sql.NVarChar)

  await preparedStatement.prepare(`
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
            -- Ignore locked records
            with (readpast)
            where
              t.timeseries_header_id = th.id
            union
            select
              source_id,
              source_type
            from
              fff_staging.v_active_timeseries_staging_exception tse
            -- Ignore locked records
            with (readpast)
            where
              tse.timeseries_header_id = th.id
          )
        )
  `)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    sourceId: taskRunData.message.plotId ? taskRunData.message.plotId : taskRunData.message.filterId,
    sourceType: taskRunData.message.plotId ? 'P' : 'F'
  }

  const result = await preparedStatement.execute(parameters)
  return Promise.resolve(!!(result.recordset && result.recordset[0]))
}
