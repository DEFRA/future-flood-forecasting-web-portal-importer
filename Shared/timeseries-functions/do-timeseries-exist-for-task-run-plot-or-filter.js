const { executePreparedStatementInTransaction } = require('../transaction-helper')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  return Promise.resolve(await executePreparedStatementInTransaction(doTimeseriesExistForTaskRunPlotOrFilter, context, taskRunData.transaction, taskRunData))
}

async function doTimeseriesExistForTaskRunPlotOrFilter (context, preparedStatement, taskRunData) {
  const fewsParameters = taskRunData.plotId ? `&plotId=${taskRunData.plotId}%` : `&filterId=${taskRunData.filterId}%`
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('fewsParameters', sql.NVarChar)

  await preparedStatement.prepare(`
    select
      t.id
    from
      fff_staging.timeseries_header th,
      fff_staging.timeseries t
     where
      th.task_run_id = @taskRunId and
      th.id = t.timeseries_header_id and
      t.fews_parameters like @fewsParameters     
    `)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    fewsParameters: fewsParameters
  }

  const result = await preparedStatement.execute(parameters)

  return !!(result && result.recordset && result.recordset[0])
}
