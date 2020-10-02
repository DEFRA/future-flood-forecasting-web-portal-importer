const sql = require('mssql')
const { doInTransaction, executePreparedStatementInTransaction } = require('../transaction-helper')
const StagingError = require('./staging-error')

module.exports = async function (context, stagingExceptionData) {
  const transaction = stagingExceptionData.transaction
  await transaction.rollback()
  await doInTransaction(createStagingExceptionInTransaction, context, 'Unable to create staging exception', null, stagingExceptionData)
  if (stagingExceptionData.throwStagingErrorFollowingStagingExceptionCreation) {
    throw new StagingError(stagingExceptionData.errorMessage)
  } else {
    context.log.error(stagingExceptionData.errorMessage)
  }
}

async function createStagingExceptionInTransaction (transaction, context, stagingExceptionData) {
  const newStagingExceptionData = Object.assign({}, stagingExceptionData)
  newStagingExceptionData.transaction = transaction
  await executePreparedStatementInTransaction(createStagingException, context, transaction, newStagingExceptionData)
}

async function createStagingException (context, preparedStatement, stagingExceptionData) {
  context.log.error(stagingExceptionData.errorMessage)

  await preparedStatement.input('payload', sql.NVarChar)
  await preparedStatement.input('description', sql.NVarChar)
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('sourceFunction', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(`
    insert into
      fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id)
    values
     (@payload, @description, @taskRunId, @sourceFunction, @workflowId)
  `)

  const parameters = {
    payload: typeof (stagingExceptionData.message) === 'string' ? stagingExceptionData.message : JSON.stringify(stagingExceptionData.message),
    description: stagingExceptionData.errorMessage,
    taskRunId: stagingExceptionData.taskRunId || null,
    sourceFunction: stagingExceptionData.sourceFunction || null,
    workflowId: stagingExceptionData.workflowId || null
  }

  await preparedStatement.execute(parameters)
}
