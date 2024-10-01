import { executePreparedStatementInTransaction } from '../../Shared/transaction-helper.js'
import sql from 'mssql'

const query = `
  insert into
    fff_staging.inactive_timeseries_staging_exception (timeseries_staging_exception_id)
  select
    tse.id
  from
    fff_staging.timeseries_staging_exception tse
    inner join fff_staging.timeseries_header th
      on tse.timeseries_header_id = th.id  
  where
    th.id = tse.timeseries_header_id and
    th.task_run_id = @taskRunId and
    tse.source_id = @sourceId and
    tse.source_type = @sourceType and
    not exists
      (
        select
          1
        from
          fff_staging.inactive_timeseries_staging_exception itse
        where
          itse.timeseries_staging_exception_id = tse.id
      )
`

export default async function (context, taskRunData) {
  await executePreparedStatementInTransaction(deactivateTimeseriesStagingExceptionsForTaskRunPlotOrFilter, context, taskRunData.transaction, taskRunData)
}

async function deactivateTimeseriesStagingExceptionsForTaskRunPlotOrFilter (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('sourceId', sql.NVarChar)
  await preparedStatement.input('sourceType', sql.NVarChar)

  await preparedStatement.prepare(query)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    sourceId: taskRunData.message.plotId ? taskRunData.message.plotId : taskRunData.message.filterId,
    sourceType: taskRunData.message.plotId ? 'P' : 'F'
  }

  await preparedStatement.execute(parameters)
}
