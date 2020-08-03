const sql = require('mssql')

module.exports = async function (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(`
    select 
      filter_id
    from
      fff_staging.non_display_group_workflow
    where
      workflow_id = @workflowId
    except
    select
      substring(t.fews_parameters, 11, charindex('&', t.fews_parameters, 2) - 11) as filter_id
    from
      fff_staging.timeseries_header th,
      fff_staging.timeseries t
    where
      th.id = t.timeseries_header_id and
      th.task_run_id = @taskRunId
    except
      select
        substring(tse.fews_parameters, 11, charindex('&', tse.fews_parameters, 2) - 11) as filter_id
      from
        fff_staging.timeseries_header th,
        fff_staging.timeseries_staging_exception tse
      where
        th.id = tse.timeseries_header_id and
        th.task_run_id = @taskRunId  
  `)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  for (let record of result.recordset) {
    taskRunData.unprocessedFilterIds.push(record.filter_id)
  }
}
