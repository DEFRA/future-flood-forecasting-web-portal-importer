const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(deactivateObsoleteTimeseriesStagingExceptionsForWorkflowPlotOrFilter, context, taskRunData.transaction, taskRunData)
}

async function deactivateObsoleteTimeseriesStagingExceptionsForWorkflowPlotOrFilter (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunCompletionTime', sql.DateTimeOffset)
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.input('sourceId', sql.NVarChar)
  await preparedStatement.input('sourceType', sql.NVarChar)

  await preparedStatement.prepare(`
    update
      tse
    set
      tse.active = 0  
    from
      fff_staging.timeseries_staging_exception tse
      inner join fff_staging.timeseries_header th
        on tse.timeseries_header_id = th.id  
    where
      th.id = tse.timeseries_header_id and
      tse.source_id = @sourceId and
      tse.source_type = @sourceType and
      th.workflow_id = @workflowId and
      th.task_completion_time < @taskRunCompletionTime and
      th.task_run_id <> @taskRunId and
      tse.active = 1
  `)

  const parameters = {
    workflowId: taskRunData.workflowId,
    taskRunId: taskRunData.taskRunId,
    taskRunCompletionTime: taskRunData.taskRunCompletionTime,
    sourceId: taskRunData.message.plotId ? taskRunData.message.plotId : taskRunData.message.filterId,
    sourceType: taskRunData.message.plotId ? 'P' : 'F'
  }

  // Temporary patch to disable deactivation on Azure while lock timeouts caused by parallel processing are resolved.
  if (process.env['SQLDB_CONNECTION_STRING'].includes('localhost')) {
    await preparedStatement.execute(parameters)
  }
}
