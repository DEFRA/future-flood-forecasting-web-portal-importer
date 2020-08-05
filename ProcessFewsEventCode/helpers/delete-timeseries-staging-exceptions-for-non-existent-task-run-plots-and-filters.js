const sql = require('mssql')

const timeseriesStagingExceptionsForTaskRunQuery = `
  select 
    source_id,
    source_type
  from
    fff_staging.timeseries_staging_exception tse,
    fff_staging.timeseries_header th
  where
    th.id = tse.timeseries_header_id and
    th.task_run_id = @taskRunId and
    source_type = 'F'
`

// Hold a table lock on the workflow view held for the duration of the transaction to guard
// against a workflow view refresh during processing.
const deletionQuery = `
  delete
    tse
  from   
    fff_staging.timeseries_staging_exception tse
    inner join
    -- Remove the set of filters linked to TIMESERIES_STAGING_EXCEPTIONS associated
    -- with the set of filters in the current non-display group CSV from the set of filters linked to
    -- TIMESERIES_STAGING_EXCEPTIONS.
    -- Anything remaining filters have been removed from the current non-display group CSV and
    -- associated TIMESERIES_STAGING_EXCEPTIONS can be deleted accordingly.
    -- This scenario will happen when filter ID typos are corrected.   
    (
      ${timeseriesStagingExceptionsForTaskRunQuery.replace(/^/, '      ')}
      except
      (
        select
          source_id,
          source_type
        from
          fff_staging.v_workflow
        with
          (tablock holdlock)
        where
          workflow_id = @workflowId            
        intersect
        ${timeseriesStagingExceptionsForTaskRunQuery.replace(/^/, '        ')}  
      )
    ) dtse -- Timeseries staging exceptions to be deleted 
    on tse.source_id = dtse.source_id and
       tse.source_type = dtse.source_type
`
module.exports = async function (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.prepare(deletionQuery)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId
  }

  await preparedStatement.execute(parameters)
}
