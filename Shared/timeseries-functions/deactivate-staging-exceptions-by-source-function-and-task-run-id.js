const sql = require('mssql')
const { executePreparedStatementInTransaction } = require('../transaction-helper')

module.exports = async function (context, stagingExceptionData) {
  const transaction = stagingExceptionData.transaction
  await executePreparedStatementInTransaction(deactivateStagingException, context, transaction, stagingExceptionData)
}

async function deactivateStagingException (context, preparedStatement, stagingExceptionData) {
  await preparedStatement.input('sourceFunction', sql.NVarChar)
  await preparedStatement.input('taskRunId', sql.NVarChar)

  await preparedStatement.prepare(`
    update
      fff_staging.staging_exception
    set
      active = 0
    where
      task_run_id = @taskRunId and
      coalesce(
        source_function,
        convert(nvarchar(1), case
          when payload like '%description%' then 'P'
          when payload like '%taskRunId%' then 'I'
          end
        )
      ) = @sourceFunction
  `)

  const parameters = {
    taskRunId: stagingExceptionData.taskRunId,
    sourceFunction: stagingExceptionData.sourceFunction
  }

  await preparedStatement.execute(parameters)
}
