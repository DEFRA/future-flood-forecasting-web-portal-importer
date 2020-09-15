const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

const activeTimeseriesStagingExceptionsForTaskRunQuery = `
  select 
    source_id,
    source_type
  from
    fff_staging.v_active_timeseries_staging_exception tse,
    fff_staging.timeseries_header th
  where
    th.id = tse.timeseries_header_id and
    th.task_run_id = @taskRunId
`

// Note that table locks are held on each table used by the workflow view for the duration of the transaction to
// guard against a workflow table refresh during processing.
const deactivationQuery = `
  update
    tse
  set
    tse.active = 0
  from   
    fff_staging.timeseries_staging_exception tse
    inner join fff_staging.timeseries_header th on th.id = tse.timeseries_header_id
    inner join
    -- Remove the set of plots/filters linked to active TIMESERIES_STAGING_EXCEPTIONS associated
    -- with the set of plots/filters in the current workflow CSVs from the set of plots/filters linked to
    -- active TIMESERIES_STAGING_EXCEPTIONS.
    -- Any remaining plots/filters have been removed from the current workflow CSVs and
    -- associated active TIMESERIES_STAGING_EXCEPTIONS can be deactivated accordingly.
    -- This scenario will happen when plots/filter ID typos are corrected.
    (
      ${activeTimeseriesStagingExceptionsForTaskRunQuery.replace(/^/, '      ')}
      except
      (
        select
          source_id,
          source_type
        from
          fff_staging.v_workflow
        where
          workflow_id = @workflowId            
        intersect
        ${activeTimeseriesStagingExceptionsForTaskRunQuery.replace(/^/, '        ')}  
      )
    ) dtse -- Timeseries staging exceptions to be deactivated 
    on tse.source_id = dtse.source_id and
       tse.source_type = dtse.source_type
  where
    tse.active = 1
`
module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(deactivateTimeseriesStagingExceptionsForNonExistentTaskRunPlotsAndFilters, context, taskRunData.transaction, taskRunData)
}

async function deactivateTimeseriesStagingExceptionsForNonExistentTaskRunPlotsAndFilters (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(deactivationQuery)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId
  }

  await preparedStatement.execute(parameters)
}
