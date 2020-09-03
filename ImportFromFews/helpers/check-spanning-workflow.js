const sql = require('mssql')

module.exports = async function (context, preparedStatement, taskRunData) {
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(`
    select
      case when
        exists (
          select *
          from 
            fff_staging.non_display_group_workflow n
          inner join
            fff_staging.coastal_display_group_workflow c
          on 
            n.workflow_id = c.workflow_id
          where 
              n.workflow_id = @workflowId
        ) 
        or
        exists (
          select *
          from
            fff_staging.non_display_group_workflow n
          inner join
            fff_staging.fluvial_display_group_workflow f
          on n.workflow_id = f.workflow_id
            where 
          n.workflow_id = @workflowId
        )
      then
        1
      else
        0
      end
      as 
        span_workflow
  `)

  const parameters = {
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  if (result && result.recordset && result.recordset[0].span_workflow === 1) {
    taskRunData.spanWorkflow = true
  } else {
    taskRunData.spanWorkflow = false
  }
}
