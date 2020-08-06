const sql = require('mssql')
const TimeseriesStagingError = require('./timeseries-staging-error')

module.exports = async function getWorkflowOffsetData (context, preparedStatement, taskRunData) {
  await preparedStatement.input('workflowId', sql.NVarChar)
  // Run the query within a transaction with a table lock held for the duration of the transaction to guard
  // against a non display group data refresh during data retrieval.
  await preparedStatement.prepare(`
    select top (1)
      start_time_offset_hours,
      end_time_offset_hours
    from
      fff_staging.non_display_group_workflow
    with
      (tablock holdlock)
    where
      workflow_id = @workflowId
  `)
  const parameters = {
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  if (result && result.recordset && result.recordset[0]) {
    taskRunData.offsetData = {
      startTimeOffset: result.recordset[0].start_time_offset_hours,
      endTimeOffset: result.recordset[0].end_time_offset_hours
    }
  } else {
    const errorDescription = `Unable to find offset data for the workflow ${taskRunData.workflowId} of task run ${taskRunData.taskRunId} in the non-display group CSV`
    const errorData = {
      sourceId: taskRunData.sourceId,
      sourceType: taskRunData.sourceType,
      csvError: true,
      csvType: 'N',
      fewsParameters: null,
      payload: taskRunData.message,
      timeseriesHeaderId: taskRunData.timeseriesHeaderId,
      description: errorDescription
    }
    throw new TimeseriesStagingError(errorData, errorDescription)
  }
}
