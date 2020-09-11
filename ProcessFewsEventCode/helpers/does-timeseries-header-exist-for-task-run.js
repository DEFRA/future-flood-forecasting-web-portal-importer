const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  return Promise.resolve(await executePreparedStatementInTransaction(doesTimeseriesHeaderExistForTaskRun, context, taskRunData.transaction, taskRunData))
}

async function doesTimeseriesHeaderExistForTaskRun (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)

  await preparedStatement.prepare(`
      select
        id
      from
        fff_staging.timeseries_header
      where
        task_run_id = @taskRunId     
    `)

  const parameters = {
    taskRunId: taskRunData.taskRunId
  }

  const result = await preparedStatement.execute(parameters)

  return !!(result.recordset && result.recordset[0])
}
