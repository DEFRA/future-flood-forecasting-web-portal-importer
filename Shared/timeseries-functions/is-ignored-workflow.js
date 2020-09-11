const { executePreparedStatementInTransaction } = require('../transaction-helper')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  return Promise.resolve(await executePreparedStatementInTransaction(isIgnoredWorkflow, context, taskRunData.transaction, taskRunData))
}

async function isIgnoredWorkflow (context, preparedStatement, taskRunData) {
  // Run the query to retrieve ignored workflow data within a transaction with a table lock held
  // for the duration of the transaction to guard against an ignored workflow data refresh during
  // data retrieval.
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(`
    select
      workflow_id
    from
      fff_staging.ignored_workflow
    with
      (tablock holdlock)
    where
      workflow_id = @workflowId
  `)

  const parameters = {
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)
  return !!(result.recordset && result.recordset[0])
}
