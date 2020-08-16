const moment = require('moment')
const sql = require('mssql')
const doTimeseriesExistForTaskRunPlotOrFilter = require('./do-timeseries-exist-for-task-run-plot-or-filter')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const { getEnvironmentVariableAsAbsoluteInteger, getOffsetAsAbsoluteInteger } = require('../../Shared/utils')
const getFewsTimeParameter = require('./get-fews-time-parameter')
const TimeseriesStagingError = require('./timeseries-staging-error')
const getCustomOffsets = require('./get-workflow-offset-data')

const aggregatedCoastalDisplayGroupWorkflowLocationsQuery = `
  select
    'C' as csv_type,
    location_ids
  from
    fff_staging.coastal_display_group_workflow
  where
    workflow_id = @workflowId and
    plot_id = @plotId
`
const aggregatedFluvialDisplayGroupWorkflowLocationsQuery = `
  select
    'F' as csv_type,
    location_ids
  from
    fff_staging.fluvial_display_group_workflow
  where
    workflow_id = @workflowId and
    plot_id = @plotId
`
const unaggregatedCoastalDisplayGroupWorkflowLocationsQuery = `
  select
    value as location_id
  from
    fff_staging.coastal_display_group_workflow
      cross apply string_split(location_ids, ';')
  where
    workflow_id = @workflowId and
    plot_id = @plotId
`
const unaggregatedFluvialDisplayGroupWorkflowLocationsQuery = `
  select
    value as location_id
  from
    fff_staging.fluvial_display_group_workflow
      cross apply string_split(location_ids, ';')
  where
    workflow_id = @workflowId and
    plot_id = @plotId
`
const importedLocationsForTaskRunQuery = `
  select
    value as location_id
  from
    fff_staging.timeseries t
      cross apply string_split(
        replace(
          substring(fews_parameters, charindex('&locationIds=', fews_parameters),  charindex('&startTime=', fews_parameters) - charindex('&locationIds=', fews_parameters)),
          '&locationIds=',
          ';'
        ),
        ';'
      ),
    fff_staging.timeseries_header th
  where
    th.task_run_id = @taskRunId and
    th.workflow_id = @workflowId and
    th.id = t.timeseries_header_id
`

const displayGroupWorkflowQueries = {
  'C': {
    aggregatedLocations: aggregatedCoastalDisplayGroupWorkflowLocationsQuery,
    unaggregatedLocations: unaggregatedCoastalDisplayGroupWorkflowLocationsQuery
  },
  'F': {
    aggregatedLocations: aggregatedFluvialDisplayGroupWorkflowLocationsQuery,
    unaggregatedLocations: unaggregatedFluvialDisplayGroupWorkflowLocationsQuery
  }
}

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
  // Check if the workflow includes non-display group filters, if so inherit the ndg offset values
  if (taskRunData.spanWorkflow && taskRunData.spanWorkflow === true) {
    // check if there is a custom offset specified for the non-display group workflow, if not inherit the default offset
    await executePreparedStatementInTransaction(getCustomOffsets, context, taskRunData.transaction, taskRunData)
    let startTimeOffsetHours
    let endTimeOffsetHours
    if (taskRunData.offsetData.startTimeOffset && taskRunData.offsetData.startTimeOffset !== 0) {
      startTimeOffsetHours = getOffsetAsAbsoluteInteger(taskRunData.offsetData.startTimeOffset, taskRunData)
    } else {
      startTimeOffsetHours = getEnvironmentVariableAsAbsoluteInteger('FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS') || 24
    }
    if (taskRunData.offsetData.endTimeOffset && taskRunData.offsetData.endTimeOffset !== 0) {
      endTimeOffsetHours = getOffsetAsAbsoluteInteger(taskRunData.offsetData.endTimeOffset, taskRunData)
    } else {
      endTimeOffsetHours = 0 // the non display group default
    }
    taskRunData.startTime = moment(taskRunData.taskRunCompletionTime).subtract(startTimeOffsetHours, 'hours').toISOString()
    taskRunData.endTime = moment(taskRunData.taskRunCompletionTime).add(endTimeOffsetHours, 'hours').toISOString()
  } else {
    const startTimeOffsetHours = getEnvironmentVariableAsAbsoluteInteger('FEWS_DISPLAY_GROUP_START_TIME_OFFSET_HOURS') || 14
    const endTimeOffsetHours = getEnvironmentVariableAsAbsoluteInteger('FEWS_DISPLAY_GROUP_END_TIME_OFFSET_HOURS') || 120
    taskRunData.startTime = moment(taskRunData.taskRunCompletionTime).subtract(startTimeOffsetHours, 'hours').toISOString()
    taskRunData.endTime = moment(taskRunData.taskRunCompletionTime).add(endTimeOffsetHours, 'hours').toISOString()
  }
}

async function buildFewsTimeParameters (context, taskRunData) {
  taskRunData.fewsStartTime = await getFewsTimeParameter(context, taskRunData.startTime, 'startTime')
  taskRunData.fewsEndTime = await getFewsTimeParameter(context, taskRunData.endTime, 'endTime')
}

async function buildPiServerUrlIfPossible (context, taskRunData) {
  await buildLocationsToImportForWorkflowPlot(context, taskRunData)

  if (taskRunData.locationIds) {
    const buildPiServerUrlCall = taskRunData.buildPiServerUrlCalls[taskRunData.piServerUrlCallsIndex]
    await buildTimeParameters(context, taskRunData)
    const plotId = `&plotId=${taskRunData.plotId}`
    const locationIds = `&locationIds=${taskRunData.locationIds.replace(/;/g, '&locationIds=')}`
    buildPiServerUrlCall.fewsParameters = `${plotId}${locationIds}${taskRunData.fewsStartTime}${taskRunData.fewsEndTime}`
    // Construct the URL used to retrieve timeseries display groups for the configured plot, locations and date range.
    buildPiServerUrlCall.fewsPiUrl =
      encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries/displaygroups?useDisplayUnits=false
        &showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON${buildPiServerUrlCall.fewsParameter}`)
  } else {
    context.log(`Ignoring message for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})` +
      `- Timeseries for all locations have been imported`)
  }
}

async function buildCsvTypeForWorkflowPlot (context, preparedStatement, taskRunData) {
  await preparedStatement.input('plotId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(`
    select
      csv_type
    from
      fff_staging.v_workflow
    with
      (tablock holdlock)
    where
<<<<<<< HEAD
      th.workflow_id = dgw.workflow_id and
      th.id = @timeseriesHeaderId and
      th.workflow_id = @workflowId
=======
      workflow_id = @workflowId and
      source_id = @plotId and
      source_type = 'P'
>>>>>>> Facilitate replay for plots when a subset of locations has been loaded
  `)

  const parameters = {
    plotId: taskRunData.plotId,
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)
  await processBuildCsvTypeForWorkflowPlotResult(context, taskRunData, result)
}

async function processBuildCsvTypeForWorkflowPlotResult (context, taskRunData, result) {
  if (result && result.recordset && result.recordset.length === 1) {
    taskRunData.csvType = result.recordset[0].csv_type
  } else {
    const errorDescription = result.recordset.length === 0
      ? `Unable to find locations for plot ${taskRunData.plotId} of task run ${taskRunData.taskRunId} in any display group CSV`
      : `Found locations for plot ${taskRunData.plotId} of task run ${taskRunData.taskRunId} in coastal and fluvial display group CSVs`

    const errorData = {
      transaction: taskRunData.transaction,
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

async function buildAllLocationsForWorkflowPlot (context, preparedStatement, taskRunData) {
  await preparedStatement.input('plotId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(displayGroupWorkflowQueries[taskRunData.csvType].aggregatedLocations)

  const parameters = {
    plotId: taskRunData.plotId,
    workflowId: taskRunData.workflowId
  }

  return preparedStatement.execute(parameters)
}

async function buildUnprocessedPlotLocationsForTaskRunQuery (context, taskRunData) {
  return Promise.resolve(`
    -- Aggregate the set of locations to be imported for the current task run of the workflow in preparation for
    -- data retrieval from the PI Server.
    select
      string_agg(location_id, ';') as location_ids
    from (
      -- The set of locations to be imported for the current task run of the workflow is the difference between all
      -- locations linked to the workflow and the set of locations already imported for the current task run of the workflow.
      ${(displayGroupWorkflowQueries[taskRunData.csvType].unaggregatedLocations).replace(/^/, '    ')}
      except
      ${importedLocationsForTaskRunQuery.replace(/^/, '    ')}
    ) l
  `)
}

async function buildUnprocessedPlotLocationsForTaskRun (context, preparedStatement, taskRunData) {
  const locationsToImportForWorkflowPlotQuery = await buildUnprocessedPlotLocationsForTaskRunQuery(context, taskRunData)
  await preparedStatement.input('plotId', sql.NVarChar)
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(locationsToImportForWorkflowPlotQuery)

  const parameters = {
    plotId: taskRunData.plotId,
    workflowId: taskRunData.workflowId,
    taskRunId: taskRunData.taskRunId
  }

  return preparedStatement.execute(parameters)
}

async function buildLocationsToImportForWorkflowPlot (context, taskRunData) {
  await executePreparedStatementInTransaction(buildCsvTypeForWorkflowPlot, context, taskRunData.transaction, taskRunData)
  let locationsToImportForWorkflowPlotFunction
  if (await doTimeseriesExistForTaskRunPlotOrFilter(context, taskRunData)) {
    locationsToImportForWorkflowPlotFunction = buildUnprocessedPlotLocationsForTaskRun
  } else {
    locationsToImportForWorkflowPlotFunction = buildAllLocationsForWorkflowPlot
  }

  const result = await executePreparedStatementInTransaction(locationsToImportForWorkflowPlotFunction, context, taskRunData.transaction, taskRunData)

  if (result && result.recordset && result.recordset.length === 1) {
    taskRunData.locationIds = result.recordset[0].location_ids
  }
}
