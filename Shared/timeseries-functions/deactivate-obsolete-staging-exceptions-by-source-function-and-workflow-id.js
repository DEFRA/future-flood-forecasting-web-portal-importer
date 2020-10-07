const sql = require('mssql')
const { executePreparedStatementInTransaction } = require('../transaction-helper')
const isLatestTaskRunForWorkflow = require('./is-latest-task-run-for-workflow')

module.exports = async function (context, stagingExceptionData) {
  const transaction = stagingExceptionData.transaction
  const forecastWorkflow = await executePreparedStatementInTransaction(isForecastWorkflow, context, transaction, stagingExceptionData)

  if (forecastWorkflow && await isLatestTaskRunForWorkflow(context, stagingExceptionData)) {
    await executePreparedStatementInTransaction(deactivateObsoleteStagingExceptions, context, transaction, stagingExceptionData)
  }
}

async function isForecastWorkflow (context, preparedStatement, stagingExceptionData) {
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(`
    select
      cdgw.workflow_id
    from
      fff_staging.coastal_display_group_workflow cdgw
    where
      cdgw.workflow_id = @workflowId
    union
    select
      fdgw.workflow_id
    from
      fff_staging.fluvial_display_group_workflow fdgw
    where
      fdgw.workflow_id = @workflowId
    union
    select
      ndgw.workflow_id
    from
      fff_staging.non_display_group_workflow ndgw
    where
      ndgw.workflow_id = @workflowId and
      ndgw.timeseries_type in ('external_forecasting', 'simulated_forecasting')
  `)

  const parameters = {
    workflowId: stagingExceptionData.workflowId
  }

  const result = await preparedStatement.execute(parameters)
  return !!(result.recordset && result.recordset[0])
}

async function deactivateObsoleteStagingExceptions (context, preparedStatement, stagingExceptionData) {
  await preparedStatement.input('latestTaskRunCompletionTime', sql.DateTime2)
  await preparedStatement.input('sourceFunction', sql.NVarChar)
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(`
    insert into
      fff_staging.inactive_staging_exception (staging_exception_id)
    select
      se.id
    from
      fff_staging.staging_exception se
    where
      se.workflow_id = @workflowId and
      se.task_run_id <> @taskRunId and
      se.exception_time < convert(datetime2, @latestTaskRunCompletionTime, 126) at time zone 'UTC' and
      coalesce(
        source_function,
        convert(nvarchar(1), case
          when se.payload like '%description%' then 'P'
          when se.payload like '%taskRunId%' then 'I'
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
  `)

  const parameters = {
    latestTaskRunCompletionTime: stagingExceptionData.latestTaskRunCompletionTime,
    taskRunId: stagingExceptionData.taskRunId,
    workflowId: stagingExceptionData.workflowId,
    sourceFunction: stagingExceptionData.sourceFunction
  }

  await preparedStatement.execute(parameters)
}
