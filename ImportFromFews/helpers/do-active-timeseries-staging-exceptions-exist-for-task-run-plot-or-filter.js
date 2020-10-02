const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  return Promise.resolve(await executePreparedStatementInTransaction(doActiveTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter, context, taskRunData.transaction, taskRunData))
}

async function doActiveTimeseriesStagingExceptionsExistForTaskRunPlotOrFilter (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('sourceId', sql.NVarChar)
  await preparedStatement.input('sourceType', sql.NChar)

  await preparedStatement.prepare(`
    select
      tse.id
    from
      fff_staging.timeseries_header th,
      fff_staging.timeseries_staging_exception tse
    where
      th.task_run_id = @taskRunId and
      th.id = tse.timeseries_header_id and
      tse.source_id = @sourceId and
      tse.source_type = @sourceType and
      (
        fff_staging.is_timeseries_staging_exception_active(tse.id)
      ) = 1
    `)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    sourceId: taskRunData.sourceId,
    sourceType: taskRunData.sourceType
  }

  const result = await preparedStatement.execute(parameters)

  return !!(result && result.recordset && result.recordset[0])
}
