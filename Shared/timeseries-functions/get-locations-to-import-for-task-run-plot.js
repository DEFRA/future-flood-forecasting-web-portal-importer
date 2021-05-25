const sql = require('mssql')
const doTimeseriesExistForTaskRunPlotOrFilter = require('./do-timeseries-exist-for-task-run-plot-or-filter')
const { executePreparedStatementInTransaction } = require('../transaction-helper')
const TimeseriesStagingError = require('./timeseries-staging-error')

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
  C: {
    aggregatedLocations: aggregatedCoastalDisplayGroupWorkflowLocationsQuery,
    unaggregatedLocations: unaggregatedCoastalDisplayGroupWorkflowLocationsQuery
  },
  F: {
    aggregatedLocations: aggregatedFluvialDisplayGroupWorkflowLocationsQuery,
    unaggregatedLocations: unaggregatedFluvialDisplayGroupWorkflowLocationsQuery
  }
}

module.exports = async function (context, taskRunData) {
  return Promise.resolve(await buildLocationsToImportForTaskRunPlot(context, taskRunData))
}

async function buildLocationsToImportForTaskRunPlot (context, taskRunData) {
  await executePreparedStatementInTransaction(buildCsvTypeForWorkflowPlot, context, taskRunData.transaction, taskRunData)
  let locationsToImportForTaskRunPlotFunction
  if (await doTimeseriesExistForTaskRunPlotOrFilter(context, taskRunData)) {
    locationsToImportForTaskRunPlotFunction = buildUnprocessedPlotLocationsForTaskRun
  } else if (taskRunData.getAllLocationsForWorkflowPlotWhenNoTimeseriesExist) {
    locationsToImportForTaskRunPlotFunction = buildAllLocationsForWorkflowPlot
  }

  if (locationsToImportForTaskRunPlotFunction) {
    return await retrieveLocationsToImportForTaskRunPlot(context, taskRunData, locationsToImportForTaskRunPlotFunction)
  }
}

async function retrieveLocationsToImportForTaskRunPlot (context, taskRunData, locationsToImportForTaskRunPlotFunction) {
  const result = await executePreparedStatementInTransaction(locationsToImportForTaskRunPlotFunction, context, taskRunData.transaction, taskRunData)

  if (result && result.recordset && result.recordset.length === 1) {
    return result.recordset[0].location_ids
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
    where
      workflow_id = @workflowId and
      source_id = @plotId and
      source_type = 'P'
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
