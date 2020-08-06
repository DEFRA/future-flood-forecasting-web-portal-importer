const moment = require('moment')
const sql = require('mssql')
const { getEnvironmentVariableAsInteger, getOffsetAsInterger } = require('../../Shared/utils')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const getFewsTimeParameter = require('./get-fews-time-parameter')
const TimeseriesStagingError = require('./timeseries-staging-error')
const getCustomOffsets = require('./get-workflow-offset-data')

module.exports = async function (context, taskRunData) {
  if (taskRunData.approved) {
    await buildPiServerUrlIfPossible(context, taskRunData)
  } else {
    context.log.warn(`Ignoring message for plot ${taskRunData.plotId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) - forecast is not approved`)
  }
}

async function buildTimeParameters (context, taskRunData) {
  await buildStartAndEndTimes(context, taskRunData)
  await buildFewsTimeParameters(context, taskRunData)
}

async function buildStartAndEndTimes (context, taskRunData) {
  // Check if the workflow spans into NDG's, if it does inherit the first filters offset values
  // have a look at the ndg tabe and see if the workflow exists
  // if it does grab the first offsets values (as all should be the same for a workflow)
  // a flag passed through to say the plot is part of a spanned workflow would be useful
  if (taskRunData.message.spanWorkflow && taskRunData.message.spanWorkflow === true) {
    // if there is a custom offset for non display groups
    await executePreparedStatementInTransaction(getCustomOffsets, context, taskRunData.transaction, taskRunData)
    let startTimeOffsetHours
    let endTimeOffsetHours
    if (taskRunData.offsetData.startTimeOffset && taskRunData.offsetData.startTimeOffset !== 0) {
      startTimeOffsetHours = getOffsetAsInterger(taskRunData.offsetData.startTimeOffset, taskRunData)
    } else {
      startTimeOffsetHours = getEnvironmentVariableAsInteger('FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS') || 24
    }
    if (taskRunData.offsetData.endTimeOffset && taskRunData.offsetData.endTimeOffset !== 0) {
      endTimeOffsetHours = getOffsetAsInterger(taskRunData.offsetData.endTimeOffset, taskRunData)
    } else {
      endTimeOffsetHours = 0 // The non display group default
    }
    taskRunData.startTime = moment(taskRunData.taskRunCompletionTime).subtract(startTimeOffsetHours, 'hours').toISOString()
    taskRunData.endTime = moment(taskRunData.taskRunCompletionTime).add(endTimeOffsetHours, 'hours').toISOString()
  } else {
    const startTimeOffsetHours = getEnvironmentVariableAsInteger('FEWS_DISPLAY_GROUP_START_TIME_OFFSET_HOURS') || 14
    const endTimeOffsetHours = getEnvironmentVariableAsInteger('FEWS_DISPLAY_GROUP_END_TIME_OFFSET_HOURS') || 120
    taskRunData.startTime = moment(taskRunData.taskRunCompletionTime).subtract(startTimeOffsetHours, 'hours').toISOString()
    taskRunData.endTime = moment(taskRunData.taskRunCompletionTime).add(endTimeOffsetHours, 'hours').toISOString()
  }
}

async function buildFewsTimeParameters (context, taskRunData) {
  taskRunData.fewsStartTime = await getFewsTimeParameter(context, taskRunData.startTime, 'startTime')
  taskRunData.fewsEndTime = await getFewsTimeParameter(context, taskRunData.endTime, 'endTime')
}

async function buildPiServerUrlIfPossible (context, taskRunData) {
  await executePreparedStatementInTransaction(getLocationsForWorkflowPlot, context, taskRunData.transaction, taskRunData)
  await buildTimeParameters(context, taskRunData)
  const plotId = `&plotId=${taskRunData.plotId}`
  const locationIds = `&locationIds=${taskRunData.locationIds.replace(/;/g, '&locationIds=')}`
  taskRunData.fewsParameters = `${plotId}${locationIds}${taskRunData.fewsStartTime}${taskRunData.fewsEndTime}`
  // Construct the URL used to retrieve timeseries display groups for the configured plot, locations and date range.
  taskRunData.fewsPiUrl =
    encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries/displaygroups?useDisplayUnits=false
      &showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON${taskRunData.fewsParameters}`)
}

async function getLocationsForWorkflowPlot (context, preparedStatement, taskRunData) {
  await preparedStatement.input('plotId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.input('timeseriesHeaderId', sql.NVarChar)
  // Run the query to retrieve the set of locations for the plot within a transaction with a
  // table lock held for the duration of the transaction to guard against a display group data
  // refresh during data retrieval.
  await preparedStatement.prepare(`
  select
  csv_type,
  dgw.location_ids
from
  (
    select
      'C' as csv_type,
      workflow_id,
      location_ids
    from
      fff_staging.coastal_display_group_workflow
    with
      (tablock holdlock)  
    where
      workflow_id = @workflowId and
      plot_id = @plotId   
    union
    select
      'F' as csv_type,
      workflow_id,
      location_ids
    from
      fff_staging.fluvial_display_group_workflow
    with
      (tablock holdlock)  
    where
      workflow_id = @workflowId and
      plot_id = @plotId  
  ) dgw,
  fff_staging.timeseries_header th
where
  th.workflow_id = dgw.workflow_id and
  th.id = @timeseriesHeaderId and
  th.workflow_id = @workflowId
   `)

  const parameters = {
    plotId: taskRunData.plotId,
    workflowId: taskRunData.workflowId,
    timeseriesHeaderId: taskRunData.timeseriesHeaderId
  }

  const result = await preparedStatement.execute(parameters)

  if (result && result.recordset && result.recordset.length === 1) {
    taskRunData.csvType = result.recordset[0].csv_type
    taskRunData.locationIds = result.recordset[0].location_ids
  } else {
    const errorDescription = result.recordset.length === 0
      ? `Unable to find locations for plot ${taskRunData.plotId} of task run ${taskRunData.taskRunId} in any display group CSV`
      : `Found locations for plot ${taskRunData.plotId} of task run ${taskRunData.taskRunId} in coastal and fluvial display group CSVs`

    const errorData = {
      sourceId: taskRunData.sourceId,
      sourceType: taskRunData.sourceType,
      csvError: true,
      // Either the plot exists in both the fluvial and coastal display group CSVs or cannot be found in either CSV.
      // In both cases the CSV type is unknown.
      csvType: 'U',
      fewsParameters: null,
      timeseriesHeaderId: taskRunData.timeseriesHeaderId,
      payload: taskRunData.message,
      description: errorDescription
    }
    throw new TimeseriesStagingError(errorData, errorDescription)
  }
}
