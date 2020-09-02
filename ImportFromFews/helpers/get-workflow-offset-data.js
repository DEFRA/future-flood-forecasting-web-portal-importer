const sql = require('mssql')
const TimeseriesStagingError = require('./timeseries-staging-error')

const findOffsetQuery = `
  select distinct
    start_time_offset_hours,
    end_time_offset_hours
  from
    fff_staging.non_display_group_workflow
  with
    (tablock holdlock)
  where
    workflow_id = @workflowId`

module.exports = async function (context, preparedStatement, taskRunData) {
  // Run the query within a transaction with a table lock held for the duration of the transaction to guard
  // against a non display group data refresh during data retrieval.
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(findOffsetQuery)
  const parameters = {
    workflowId: taskRunData.workflowId
  }
  const result = await preparedStatement.execute(parameters)

  if (result && result.recordset && result.recordset[0] && result.recordset.length === 1) {
    taskRunData.offsetData = {
      startTimeOffset: result.recordset[0].start_time_offset_hours,
      endTimeOffset: result.recordset[0].end_time_offset_hours
    }
  } else {
    let errorMessage
    errorMessage = `No custom offsets found for workflow: ${taskRunData.workflowId}.`
    if (result && result.recordset && result.recordset[0] && result.recordset.length > 1) {
      errorMessage = `There are multiple custom offsets (${result.recordset.length}) specified for the workflow: ${taskRunData.workflowId}.`
    }
    context.log(errorMessage)
    const errorDescription = `${errorMessage} Task run ${taskRunData.taskRunId} in the non-display group CSV`
    const errorData = {
      sourceId: taskRunData.sourceId,
      sourceType: taskRunData.sourceType,
      csvError: true,
      csvType: taskRunData.csvType || 'N',
      fewsParameters: null,
      payload: taskRunData.message,
      timeseriesHeaderId: taskRunData.timeseriesHeaderId,
      description: errorDescription
    }
    throw new TimeseriesStagingError(errorData, errorDescription)
  }
}
