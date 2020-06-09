const sql = require('mssql')

module.exports = async function getLatestTaskRunEndTime (context, preparedStatement, routeData, filter) {
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.input('filterId', sql.NVarChar)

  await preparedStatement.prepare(`
    select start_time_offset_hours, end_time_offset_hours
    from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow
    where 
      workflow_id = @workflowId
      and
      filter_id = @filterId
  `)

  const parameters = {
    workflowId: routeData.workflowId,
    filterId: filter
  }

  const result = await preparedStatement.execute(parameters)
  let overriderequired

  if (result.recordset.length > 1) {
    context.log.error(`Error: more than one filter-workflow combination found.`)
    throw new Error(`Error: more than one filter-workflow combination found.`)
  } else {
    // when zero?
    if (result.recordset && result.recordset[0].start_time_offset_hours && result.recordset[0].end_time_offset_hours) {
      routeData.ndgOversetOverrideBackward = result.recordset[0].start_time_offset_hours
      routeData.ndgOversetOverrideForward = result.recordset[0].end_time_offset_hours
      overriderequired = true
    } else {
      overriderequired = false
    }
  }

  return overriderequired
}
