const sql = require('mssql')
const { doInTransaction, executePreparedStatementInTransaction } = require('./transaction-helper')
const StagingError = require('./staging-error')

module.exports = async function (context, preparedStatement, routeData, description) {
  const transaction = preparedStatement.parent
  await transaction.rollback()
  await doInTransaction(createStagingExceptionInTransaction, context, 'Unable to create staging exception', null, routeData, description)
  throw new StagingError(description)
}

async function createStagingExceptionInTransaction (transaction, context, routeData, description) {
  await executePreparedStatementInTransaction(createStagingException, context, transaction, routeData, description)
}

async function createStagingException (context, preparedStatement, routeData, description) {
  await preparedStatement.input('payload', sql.NVarChar)
  await preparedStatement.input('description', sql.NVarChar)
  await preparedStatement.input('taskRunId', sql.NVarChar)

  await preparedStatement.prepare(`
    insert into
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception (payload, description, task_run_id)
    values
     (@payload, @description, @taskRunId)
  `)

  const parameters = {
    payload: routeData.message,
    description: description,
    taskRunId: routeData.taskRunId
  }

  await preparedStatement.execute(parameters)
}
