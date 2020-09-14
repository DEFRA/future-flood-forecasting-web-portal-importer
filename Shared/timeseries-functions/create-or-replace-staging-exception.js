const sql = require('mssql')
const deactivateStagingExceptionBySourceFunctionAndTaskRunId = require('./deactivate-staging-exceptions-by-source-function-and-task-run-id')
const { doInTransaction, executePreparedStatementInTransaction } = require('../transaction-helper')
const StagingError = require('./staging-error')

module.exports = async function (context, stagingExceptionData) {
  const transaction = stagingExceptionData.transaction
  await transaction.rollback()
  await doInTransaction(createOrReplaceStagingExceptionInTransaction, context, 'Unable to create staging exception', null, stagingExceptionData)
  if (stagingExceptionData.throwStagingErrorFollowingStagingExceptionCreation) {
    throw new StagingError(stagingExceptionData.errorMessage)
  } else {
    context.log.error(stagingExceptionData.errorMessage)
  }
}

async function createOrReplaceStagingExceptionInTransaction (transaction, context, stagingExceptionData) {
  const newStagingExceptionData = Object.assign({}, stagingExceptionData)
  newStagingExceptionData.transaction = transaction
  await deactivateStagingExceptionBySourceFunctionAndTaskRunId(context, newStagingExceptionData)
  await executePreparedStatementInTransaction(createStagingException, context, transaction, newStagingExceptionData)
}

async function createStagingException (context, preparedStatement, stagingExceptionData) {
  context.log.error(stagingExceptionData.errorMessage)

  await preparedStatement.input('payload', sql.NVarChar)
  await preparedStatement.input('description', sql.NVarChar)
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('sourceFunction', sql.NVarChar)

  await preparedStatement.prepare(`
    insert into
      fff_staging.staging_exception (payload, description, task_run_id, source_function)
    values
     (@payload, @description, @taskRunId, @sourceFunction)
  `)

  const parameters = {
    payload: typeof (stagingExceptionData.message) === 'string' ? stagingExceptionData.message : JSON.stringify(stagingExceptionData.message),
    description: stagingExceptionData.errorMessage,
    taskRunId: stagingExceptionData.taskRunId || null,
    sourceFunction: stagingExceptionData.sourceFunction || null
  }

  await preparedStatement.execute(parameters)
}