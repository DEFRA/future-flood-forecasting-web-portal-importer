const sql = require('mssql')
const { executePreparedStatementInTransaction } = require('../transaction-helper')

const query = `
  insert into
    fff_staging.inactive_staging_exception (staging_exception_id)
  select
    se.id
  from
    fff_staging.staging_exception se
  where
  task_run_id = @taskRunId and
  coalesce(
    source_function,
    convert(nvarchar(1), case
      when payload like '%description%' then 'P'
      when payload like '%taskRunId%' then 'I'
      end
    )
  ) = @sourceFunction and
  not exists
    (
      select
        1
      from
        fff_staging.inactive_staging_exception ise
      where
        ise.staging_exception_id = se.id
    )
`

module.exports = async function (context, stagingExceptionData) {
  const transaction = stagingExceptionData.transaction
  await executePreparedStatementInTransaction(deactivateStagingException, context, transaction, stagingExceptionData)
}

async function deactivateStagingException (context, preparedStatement, stagingExceptionData) {
  await preparedStatement.input('sourceFunction', sql.NVarChar)
  await preparedStatement.input('taskRunId', sql.NVarChar)

  await preparedStatement.prepare(query)

  const parameters = {
    taskRunId: stagingExceptionData.taskRunId,
    sourceFunction: stagingExceptionData.sourceFunction
  }

  await preparedStatement.execute(parameters)
}
