const moment = require('moment')
const sql = require('mssql')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const getFewsTimeParameter = require('./get-fews-time-parameter')
const TimeseriesStagingError = require('./timeseries-staging-error')

const EXTERNAL_HISTORICAL = 'external_historical'
const EXTERNAL_FORECASTING = 'external_forecasting'
const SIMULATED_FORECASTING = 'simulated_forecasting'

module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(getWorkflowFilterData, context, taskRunData.transaction, taskRunData)
  if (!taskRunData.filterData.approvalRequired || taskRunData.approved) {
    if (!taskRunData.filterData.approvalRequired) {
      context.log.info(`Filter ${taskRunData.filterId} does not requires approval.`)
    } else if (taskRunData.approved) {
      context.log.info(`Filter ${taskRunData.filterId} requires approval and has been approved.`)
    }
    await buildPiServerUrlIfPossible(context, taskRunData)
  } else {
    context.log.error(`Ignoring filter ${taskRunData.filterId}. The filter requires approval and has NOT been approved.`)
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
  let truncationOffsetHoursBackward
  let truncationOffsetHoursForward

  if (taskRunData.filterData.startTimeOffset && taskRunData.filterData.startTimeOffset !== 0) {
    truncationOffsetHoursBackward = Math.abs(taskRunData.filterData.startTimeOffset)
  } else {
    truncationOffsetHoursBackward = process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS'] ? parseInt(process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS']) : 24
  }
  if (taskRunData.filterData.endTimeOffset && taskRunData.filterData.endTimeOffset !== 0) {
    truncationOffsetHoursForward = Math.abs(taskRunData.filterData.endTimeOffset)
  } else {
    truncationOffsetHoursForward = 0
  }

  taskRunData.startTime = moment(taskRunData.startCreationTime).subtract(truncationOffsetHoursBackward, 'hours').toISOString()
  taskRunData.endTime = moment(taskRunData.endCreationTime).add(truncationOffsetHoursForward, 'hours').toISOString()
}

async function buildFewsTimeParameters (context, taskRunData) {
  // Build time parameters in the format expected by the PI Server
  taskRunData.fewsStartCreationTime = await getFewsTimeParameter(context, taskRunData.startCreationTime, 'startCreationTime')
  taskRunData.fewsEndCreationTime = await getFewsTimeParameter(context, taskRunData.endCreationTime, 'endCreationTime')
  taskRunData.fewsStartTime = await getFewsTimeParameter(context, taskRunData.startTime, 'startTime')
  taskRunData.fewsEndTime = await getFewsTimeParameter(context, taskRunData.endTime, 'endTime')
}

async function buildPiServerUrlIfPossible (context, taskRunData) {
  const filterData = taskRunData.filterData
  await buildTimeParameters(context, taskRunData)
  if (filterData.timeseriesType && (filterData.timeseriesType === EXTERNAL_HISTORICAL || filterData.timeseriesType === EXTERNAL_FORECASTING)) {
    taskRunData.fewsParameters = `&filterId=${taskRunData.filterId}${taskRunData.fewsStartTime}${taskRunData.fewsEndTime}${taskRunData.fewsStartCreationTime}${taskRunData.fewsEndCreationTime}`
  } else if (filterData.timeseriesType && filterData.timeseriesType === SIMULATED_FORECASTING) {
    taskRunData.fewsParameters = `&filterId=${taskRunData.filterId}${taskRunData.fewsStartTime}${taskRunData.fewsEndTime}`
  }

  if (taskRunData.fewsParameters) {
    taskRunData.fewsPiUrl =
      encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries?useDisplayUnits=false&showThresholds=true&showProducts=false
        &omitMissing=true&onlyHeaders=false&showEnsembleMemberIds=false&documentVersion=1.26&documentFormat=PI_JSON&forecastCount=1${taskRunData.fewsParameters}`)
  } else {
    // FEWS parameters must be specified otherwise the data return is likely to be very large
    const errorDescription = `There is no recognizable timeseries type specified for the filter ${taskRunData.filterId} in the non-display group CSV`
    const errorData = {
      sourceId: taskRunData.sourceId,
      sourceType: taskRunData.sourceType,
      csvError: true,
      csvType: 'N',
      fewsParameters: null,
      payload: taskRunData.message,
      errorData: taskRunData.message,
      timeseriesHeaderId: taskRunData.timeseriesHeaderId,
      description: errorDescription
    }
    throw new TimeseriesStagingError(errorData, errorDescription)
  }
}

async function getWorkflowFilterData (context, preparedStatement, taskRunData) {
  await preparedStatement.input('filterId', sql.NVarChar)
  await preparedStatement.input('nonDisplayGroupWorkflowId', sql.NVarChar)
  // Run the query within a transaction with a table lock held for the duration of the transaction to guard
  // against a non display group data refresh during data retrieval.
  await preparedStatement.prepare(`
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
      filter_id = @filterId

  `)
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

async function getLatestTaskRunEndTime (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)

  await preparedStatement.prepare(`
    select top(1)
      task_run_id as previous_staged_task_run_id,
      task_completion_time as previous_staged_task_completion_time
    from
      fff_staging.timeseries_header
    where
      workflow_id = @workflowId and
      task_run_id <> @taskRunId
    order by
      task_completion_time desc
  `)

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
