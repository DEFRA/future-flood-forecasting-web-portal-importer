const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

const query = `
  select
    id,
    workflow_id,
    coalesce(
      task_start_time,
      convert(datetime2, substring(message, charindex('start time: ', message) + 11, 20), 126) at time zone 'utc'
    ) as task_start_time,
    task_completion_time,
    coalesce(
      forecast,
      convert(bit, case
        when message like '%forecast:%true%' then 1
        when message like '%is made current manually%' then 1  
        when message like '%forecast:%false%' then 0
        end
      )
    ) as forecast,
    coalesce(
      approved,
      convert(bit, case
        when message like '%approved:%true%' then 1 
        when message like '%approved:%false%' then 0
        end
      )
    ) as approved      
  from
    fff_staging.timeseries_header
  where
    task_run_id = @taskRunId
`

module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(getTimeseriesHeaderData, context, taskRunData.transaction, taskRunData)
}

async function getTimeseriesHeaderData (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.prepare(query)

  const parameters = {
    taskRunId: taskRunData.taskRunId
  }

  const result = await preparedStatement.execute(parameters)

  if (result && result.recordset && result.recordset[0]) {
    taskRunData.timeseriesHeaderId = result.recordset[0].id
    taskRunData.taskRunStartTime = result.recordset[0].task_start_time
    taskRunData.taskRunCompletionTime = result.recordset[0].task_completion_time
    taskRunData.forecast = result.recordset[0].forecast
    taskRunData.approved = result.recordset[0].approved
    taskRunData.workflowId = result.recordset[0].workflow_id
  }

  return taskRunData
}
