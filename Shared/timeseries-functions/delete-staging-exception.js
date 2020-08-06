const sql = require('mssql')
const { executePreparedStatementInTransaction } = require('../transaction-helper')

module.exports = async function (context, preparedStatement, stagingExceptionData) {
  const transaction = preparedStatement.parent
  await executePreparedStatementInTransaction(deleteStagingException, context, transaction, stagingExceptionData)
}

async function deleteStagingException (context, preparedStatement, stagingExceptionData) {
  await preparedStatement.input('sourceFunction', sql.NVarChar)
  await preparedStatement.input('taskRunId', sql.NVarChar)

  await preparedStatement.prepare(`
    delete from
      fff_staging.staging_exception
    where 
      task_run_id = @taskRunId and
      source_function = @sourceFunction 
  `)

  const parameters = {
    taskRunId: stagingExceptionData.taskRunId,
    sourceFunction: stagingExceptionData.sourceFunction
  }

  await preparedStatement.execute(parameters)
}
