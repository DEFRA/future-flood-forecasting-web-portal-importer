const sql = require('mssql')

module.exports = async function getLatestTaskRunEndTime (context, preparedStatement, routeData, filter) {
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.input('filterId', sql.NVarChar)

  await preparedStatement.prepare(`
    select
      approved, timeseries_type, start_time_offset_hours, end_time_offset_hours
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

  if (result.recordset.length > 1) {
    context.log.error(`Error: more than one filter-workflow combination found.`)
    throw new Error(`Error: more than one filter-workflow combination found.`)
  } else {
    if (result.recordset[0]) {
      let startOffset = result.recordset[0].start_time_offset_hours
      let endOffset = result.recordset[0].end_time_offset_hours
      routeData.approvalRequired = result.recordset[0].approved
      routeData.timeseriesType = result.recordset[0].timeseries_type
      if (startOffset > 0) {
        routeData.ndgOversetOverrideBackward = startOffset
        routeData.startTimeOverrideRequired = true
      }
      if (endOffset > 0) {
        routeData.ndgOversetOverrideForward = endOffset
        routeData.endTimeOverrideRequired = true
      }
    }
  }
  return routeData
}
