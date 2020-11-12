const moment = require('moment')
const sql = require('mssql')
const deactivateObsoleteTimeseriesStagingExceptionsForWorkflowPlotOrFilter = require('./deactivate-obsolete-timeseries-staging-exceptions-for-workflow-plot-or-filter')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const { getEnvironmentVariableAsAbsoluteInteger, getOffsetAsAbsoluteInteger } = require('../../Shared/utils')
const getFewsTimeParameter = require('./get-fews-time-parameter')
const isLatestTaskRunForWorkflow = require('../../Shared/timeseries-functions/is-latest-task-run-for-workflow')
const timeseriesTypeConstants = require('./timeseries-type-constants')
const TimeseriesStagingError = require('../../Shared/timeseries-functions/timeseries-staging-error')

const getWorkflowFilterDataQuery = `
select
  approved,
  timeseries_type,
  start_time_offset_hours,
  end_time_offset_hours
from
  fff_staging.non_display_group_workflow
with
  (tablock holdlock)
where
  workflow_id = @nonDisplayGroupWorkflowId and
  filter_id = @filterId`

const getLatestTaskRunEndTimeQuery = `
select top(1)
  task_run_id as previous_staged_task_run_id,
  task_completion_time as previous_staged_task_completion_time
from
  fff_staging.timeseries_header
where
  workflow_id = @workflowId and
  task_completion_time < (
    select
      task_completion_time
    from
      fff_staging.timeseries_header
    where
      task_run_id = @taskRunId
  )
order by 
  task_completion_time desc`

module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(getWorkflowFilterData, context, taskRunData.transaction, taskRunData)
  if (isForecast(context, taskRunData) && await isLatestTaskRunForWorkflow(context, taskRunData)) {
    await deactivateObsoleteTimeseriesStagingExceptionsForWorkflowPlotOrFilter(context, taskRunData)
  }
  // Ensure data is not imported for out of date external/simulated forecasts.
  if (!isForecast(context, taskRunData) || await isLatestTaskRunForWorkflow(context, taskRunData)) {
    if (!taskRunData.filterData.approvalRequired || taskRunData.approved) {
      context.log.info(`The filter '${taskRunData.filterId}' requires approval: '${taskRunData.filterData.approvalRequired}', and has been approved '${taskRunData.approved}'.`)
      await buildPiServerUrlIfPossible(context, taskRunData)
    } else {
      context.log.error(`Ignoring filter ${taskRunData.filterId}. The filter requires approval and has NOT been approved.`)
    }
  } else {
    context.log.warn(`Ignoring message for filter ${taskRunData.filterId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) completed on ${taskRunData.taskRunCompletionTime}` +
      ` - ${taskRunData.latestTaskRunId} completed on ${taskRunData.latestTaskRunCompletionTime} is the latest task run for workflow ${taskRunData.workflowId}`)
  }
}

async function buildTimeParameters (context, taskRunData) {
  await executePreparedStatementInTransaction(getLatestTaskRunEndTime, context, taskRunData.transaction, taskRunData)
  await buildCreationStartAndEndTimes(context, taskRunData)
  await buildStartAndEndTimes(context, taskRunData)
  await buildFewsTimeParameters(context, taskRunData)
}

async function buildCreationStartAndEndTimes (context, taskRunData) {
  // Retrieval of timeseries associated with a single task run of a non-display group workflow needs to be based on the time
  // at which the timeseries were created in the core engine. To ensure timeseries edited manually since the previous task run
  // are also retrieved, timeseries created between the end of the previous task run and the end of the current task run of
  // the workflow are retrieved. If this is the first task run of the workflow, timeseries created during the current task run
  // of the workflow are retrieved.
  if (taskRunData.previousTaskRunCompletionTime) {
    context.log.info(`The previous task run had the id: '${taskRunData.previousTaskRunId}'. This task run finished at ${taskRunData.previousTaskRunCompletionTime}, this will be used as the starting date for the next taskrun search.`)
    taskRunData.startCreationTime = moment(taskRunData.previousTaskRunCompletionTime).toISOString()
  } else {
    context.log.info(`This is the first task run processed for the non-display group workflow: '${taskRunData.workflowId}'`)
    taskRunData.startCreationTime = moment(taskRunData.taskRunStartTime).toISOString()
  }

  taskRunData.endCreationTime = moment(taskRunData.taskRunCompletionTime).toISOString()
}

async function buildStartAndEndTimes (context, taskRunData) {
  // To try and prevent additional older timeseries created by core engine amalgamation being returned, queries to the core
  // engine PI Server restrict returned timeseries to those associated with a time period designed to exclude amalgamated
  // timeseries. By default this period runs from twenty four hours before the start of the time period for which the timeseries were created
  // in the core engine (either previous task run end time or current task run start time as defined above) through to the completion time
  // of the current task run. This time period can be overridden by the FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS environment variable.
  let truncationOffsetHoursBackward = getEnvironmentVariableAsAbsoluteInteger('FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS') || 24
  let truncationOffsetHoursForward = 0

  if (taskRunData.filterData.startTimeOffset && taskRunData.filterData.startTimeOffset !== 0) {
    truncationOffsetHoursBackward = getOffsetAsAbsoluteInteger(taskRunData.filterData.startTimeOffset, taskRunData)
  }
  if (taskRunData.filterData.endTimeOffset && taskRunData.filterData.endTimeOffset !== 0) {
    truncationOffsetHoursForward = getOffsetAsAbsoluteInteger(taskRunData.filterData.endTimeOffset, taskRunData)
  }

  if (taskRunData.filterData.timeseriesType === timeseriesTypeConstants.SIMULATED_FORECASTING) {
    // time frame search period basis is the current end time for forecast data
    taskRunData.startTime = moment(taskRunData.taskRunCompletionTime).subtract(truncationOffsetHoursBackward, 'hours').toISOString()
    taskRunData.endTime = moment(taskRunData.taskRunCompletionTime).add(truncationOffsetHoursForward, 'hours').toISOString()
  } else {
    // time frame search period basis extends to the last observed time (either the previous task run end time or the current task run start time if its the first instance of a task run/workflow)
    taskRunData.startTime = moment(taskRunData.startCreationTime).subtract(truncationOffsetHoursBackward, 'hours').toISOString()
    taskRunData.endTime = moment(taskRunData.endCreationTime).add(truncationOffsetHoursForward, 'hours').toISOString()
  }
}

async function buildFewsTimeParameters (context, taskRunData) {
  // Build time parameters in the format expected by the PI Server
  taskRunData.fewsStartCreationTime = await getFewsTimeParameter(context, taskRunData.startCreationTime, 'startCreationTime')
  taskRunData.fewsEndCreationTime = await getFewsTimeParameter(context, taskRunData.endCreationTime, 'endCreationTime')
  taskRunData.fewsStartTime = await getFewsTimeParameter(context, taskRunData.startTime, 'startTime')
  taskRunData.fewsEndTime = await getFewsTimeParameter(context, taskRunData.endTime, 'endTime')
}

async function buildPiServerUrlIfPossible (context, taskRunData) {
  const buildPiServerUrlCall = taskRunData.buildPiServerUrlCalls[taskRunData.piServerUrlCallsIndex]
  const filterData = taskRunData.filterData
  await buildTimeParameters(context, taskRunData)
  if (filterData.timeseriesType && (filterData.timeseriesType === timeseriesTypeConstants.EXTERNAL_HISTORICAL || filterData.timeseriesType === timeseriesTypeConstants.EXTERNAL_FORECASTING)) {
    buildPiServerUrlCall.fewsParameters = `&filterId=${taskRunData.filterId}${taskRunData.fewsStartTime}${taskRunData.fewsEndTime}${taskRunData.fewsStartCreationTime}${taskRunData.fewsEndCreationTime}`
  } else if (filterData.timeseriesType && filterData.timeseriesType === timeseriesTypeConstants.SIMULATED_FORECASTING) {
    buildPiServerUrlCall.fewsParameters = `&filterId=${taskRunData.filterId}${taskRunData.fewsStartTime}${taskRunData.fewsEndTime}`
  }

  if (buildPiServerUrlCall.fewsParameters) {
    buildPiServerUrlCall.fewsPiUrl =
      encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries?useDisplayUnits=false&showThresholds=true&showProducts=false
        &omitMissing=true&onlyHeaders=false&showEnsembleMemberIds=false&documentFormat=PI_JSON&forecastCount=1${buildPiServerUrlCall.fewsParameters}`)
  } else {
    // FEWS parameters must be specified otherwise the data return is likely to be very large
    const errorDescription = `There is no recognizable timeseries type specified for the filter ${taskRunData.filterId} in the non-display group CSV`
    await throwCsvError(taskRunData, errorDescription, 'N', null)
  }
}

async function getWorkflowFilterData (context, preparedStatement, taskRunData) {
  await preparedStatement.input('filterId', sql.NVarChar)
  await preparedStatement.input('nonDisplayGroupWorkflowId', sql.NVarChar)
  // Run the query within a transaction with a table lock held for the duration of the transaction to guard
  // against a non display group data refresh during data retrieval.
  await preparedStatement.prepare(getWorkflowFilterDataQuery)
  const parameters = {
    nonDisplayGroupWorkflowId: taskRunData.workflowId,
    filterId: taskRunData.filterId
  }

  const result = await preparedStatement.execute(parameters)

  if (result && result.recordset && result.recordset[0]) {
    taskRunData.filterData = {
      approvalRequired: result.recordset[0].approved,
      startTimeOffset: result.recordset[0].start_time_offset_hours,
      endTimeOffset: result.recordset[0].end_time_offset_hours,
      timeseriesType: result.recordset[0].timeseries_type
    }
  } else {
    const errorDescription = `Unable to find data for filter ${taskRunData.filterId} of task run ${taskRunData.taskRunId} in the non-display group CSV`
    await throwCsvError(taskRunData, errorDescription, 'N', null)
  }
}

async function getLatestTaskRunEndTime (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(getLatestTaskRunEndTimeQuery)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  if (result.recordset && result.recordset[0] && result.recordset[0].previous_staged_task_run_id) {
    taskRunData.previousTaskRunId = result.recordset[0].previous_staged_task_run_id
    taskRunData.previousTaskRunCompletionTime =
      moment(result.recordset[0].previous_staged_task_completion_time).toISOString()
  } else {
    taskRunData.previousTaskRunCompletionTime = null // task run not yet present in db
  }

  return taskRunData
}

function isForecast (context, taskRunData) {
  return taskRunData.filterData.timeseriesType === timeseriesTypeConstants.SIMULATED_FORECASTING || taskRunData.filterData.timeseriesType === timeseriesTypeConstants.EXTERNAL_FORECASTING
}

async function throwCsvError (taskRunData, errorDescription, csvType, fewsParameters) {
  const errorData = {
    transaction: taskRunData.transaction,
    sourceId: taskRunData.sourceId,
    sourceType: taskRunData.sourceType,
    csvError: true,
    csvType: csvType,
    fewsParameters: fewsParameters,
    payload: taskRunData.message,
    timeseriesHeaderId: taskRunData.timeseriesHeaderId,
    description: errorDescription
  }
  throw new TimeseriesStagingError(errorData, errorDescription)
}
