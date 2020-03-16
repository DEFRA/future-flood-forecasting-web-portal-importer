const sql = require('mssql')
const { doInTransaction, executePreparedStatementInTransaction } = require('./transaction-helper')
const StagingError = require('./staging-error')

module.exports = async function (context, preparedStatement, payload, description) {
  const transaction = preparedStatement.parent
  transaction.rollback()
  await doInTransaction(createStagingExceptionInTransaction, context, 'Unable to create staging exception', null, payload, description)
  throw new StagingError(description)
}

async function createStagingExceptionInTransaction (transaction, context, payload, description) {
  await executePreparedStatementInTransaction(createStagingException, context, transaction, payload, description)
}

async function createStagingException (context, preparedStatement, payload, description) {
  await preparedStatement.input('payload', sql.NVarChar)
  await preparedStatement.input('description', sql.NVarChar)

  await preparedStatement.prepare(`
    insert into
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception (payload, description)
    values
     (@payload, @description)
  `)

  const parameters = {
    payload: JSON.stringify(payload),
    description: description
  }

  await preparedStatement.execute(parameters)
}
