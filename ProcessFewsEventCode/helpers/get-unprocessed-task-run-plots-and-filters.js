const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const getItemsToBeProcessedAsArray = require('./get-items-to-be-processed-as-array')
const sql = require('mssql')

module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(getUnprocessedTaskRunPlotsAndFilters, context, taskRunData.transaction, taskRunData)
}

async function getUnprocessedTaskRunPlotsAndFilters (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)

  // Note that table locks are held on each table used by the workflow view for the duration of the transaction to
  // guard against a workflow table refresh during processing.
  await preparedStatement.prepare(`
    select distinct
      source_id,
      source_type
    from
      fff_staging.v_workflow
    where
      workflow_id = @workflowId
    except
    select
      case
        when t.fews_parameters like '&plotId=%' then substring(t.fews_parameters, 9, charindex('&', t.fews_parameters, 2) - 9)
        when t.fews_parameters like '&filterId=%' then substring(t.fews_parameters, 11, charindex('&', t.fews_parameters, 2) - 11)
        end as source_id,
      case
        when t.fews_parameters like '&plotId=%' then 'P'
        when t.fews_parameters like '&filterId=%' then 'F'
        end as source_type        
    from
      fff_staging.timeseries_header th,
      fff_staging.timeseries t
    where
      th.id = t.timeseries_header_id and
      th.task_run_id = @taskRunId
    except
      select
        tse.source_id,
        tse.source_type
      from
        fff_staging.timeseries_header th,
        fff_staging.v_active_timeseries_staging_exception tse
      where
        th.id = tse.timeseries_header_id and
        th.task_run_id = @taskRunId
  `)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)
  const unprocessedItems = await getItemsToBeProcessedAsArray(result.recordset)
  taskRunData.unprocessedItems.push(...unprocessedItems)
}
