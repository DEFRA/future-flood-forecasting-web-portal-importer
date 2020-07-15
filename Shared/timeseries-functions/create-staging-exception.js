const sql = require('mssql')
const { doInTransaction, executePreparedStatementInTransaction } = require('../transaction-helper')
const StagingError = require('./staging-error')

module.exports = async function (context, preparedStatement, stagingExceptionData, description) {
  const transaction = preparedStatement.parent
  await transaction.rollback()
  await doInTransaction(createStagingExceptionInTransaction, context, 'Unable to create staging exception', null, stagingExceptionData, description)
  if (stagingExceptionData.throwStagingErrorFollowingStagingExceptionCreation) {
    throw new StagingError(description)
  } else {
    context.log.error(description)
  }
}

async function createStagingExceptionInTransaction (transaction, context, stagingExceptionData, description) {
  await executePreparedStatementInTransaction(createStagingException, context, transaction, stagingExceptionData, description)
}

async function createStagingException (context, preparedStatement, stagingExceptionData, description) {
  await preparedStatement.input('payload', sql.NVarChar)
  await preparedStatement.input('description', sql.NVarChar)
  await preparedStatement.input('taskRunId', sql.NVarChar)

  await preparedStatement.prepare(`
    insert into
      fff_staging.staging_exception (payload, description, task_run_id)
    values
     (@payload, @description, @taskRunId)
  `)

  const parameters = {
    payload: typeof (stagingExceptionData.message) === 'string' ? stagingExceptionData.message : JSON.stringify(stagingExceptionData.message),
    description: description,
    taskRunId: stagingExceptionData.taskRunId || null
  }

  await preparedStatement.execute(parameters)
}