const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const getLocationsToImportForTaskRunPlot = require('../../Shared/timeseries-functions/get-locations-to-import-for-task-run-plot')
const TimeseriesStagingError = require('../../Shared/timeseries-functions/timeseries-staging-error')
const sql = require('mssql')

// Note that table locks are held on each table used by the workflow view for the duration of the transaction to
// guard against a workflow table refresh during processing.
const taskRunPlotsAndFiltersWithActiveTimeseriesStagingExceptionsQuery = `
  select distinct
    source_id,
    source_type
  from
    fff_staging.v_workflow
  where
    workflow_id = @workflowId
  intersect
  (
    select distinct
      tse.source_id,
      tse.source_type
    from
      fff_staging.timeseries_header th,
      fff_staging.v_active_timeseries_staging_exception tse
    where
      th.id = tse.timeseries_header_id and
      th.task_run_id = @taskRunId and
      tse.csv_error = 0
    union
    select distinct
      tse.source_id,
      tse.source_type
    from
      fff_staging.timeseries_header th,
      fff_staging.v_active_timeseries_staging_exception tse
    where
      th.id = tse.timeseries_header_id and
      th.task_run_id = @taskRunId and
      tse.csv_error = 1 and
      tse.csv_type = 'U'
    union
    select distinct
      tse.source_id,
      tse.source_type
    from
      fff_staging.timeseries_header th,
      fff_staging.v_active_timeseries_staging_exception tse
    where
      th.id = tse.timeseries_header_id and
      th.task_run_id = @taskRunId and
      tse.csv_error = 1 and
      tse.exception_time < (
        select
          wr.refresh_time
        from
          fff_staging.workflow_refresh wr
        where
          tse.csv_type = wr.csv_type
      )
  )
`
const workflowPlotsQuery = `
  select distinct
    source_id as plot_id
  from
    fff_staging.v_workflow
  where
    workflow_id = @workflowId and
    source_type = 'P'
`
module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(getTaskRunPlotsAndFiltersWithActiveTimeseriesStagingExceptions, context, taskRunData.transaction, taskRunData)

  // If individual lines of a plot based CSV loaded previously contained a typo in the plot name, it is possible
  // that task runs will have timeseries loaded for a subset of plot locations. Following typo corrections, data for
  // missing locations needs to be loaded.
  // To achieve this, prepare to send a message for each plot of the workflow with unimported location data.
  await executePreparedStatementInTransaction(getWorkflowPlots, context, taskRunData.transaction, taskRunData)
  await getUnimportedLocationsForTaskRunPlots(context, taskRunData)
}

async function getTaskRunPlotsAndFiltersWithActiveTimeseriesStagingExceptions (context, preparedStatement, taskRunData) {
  await preparedStatement.input('taskRunId', sql.NVarChar)
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(taskRunPlotsAndFiltersWithActiveTimeseriesStagingExceptionsQuery)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)

  for (const record of result.recordset) {
    taskRunData.itemsEligibleForReplay.push({
      sourceId: record.source_id,
      sourceType: record.source_type
    })
  }
}

async function getWorkflowPlots (context, preparedStatement, taskRunData) {
  await preparedStatement.input('workflowId', sql.NVarChar)
  await preparedStatement.prepare(workflowPlotsQuery)

  const parameters = {
    taskRunId: taskRunData.taskRunId,
    workflowId: taskRunData.workflowId
  }

  const result = await preparedStatement.execute(parameters)
  taskRunData.workflowPlots = []

  for (const record of result.recordset) {
    taskRunData.workflowPlots.push(record.plot_id)
  }
}

async function getUnimportedLocationsForTaskRunPlots (context, taskRunData) {
  for (const plotId of taskRunData.workflowPlots) {
    taskRunData.plotId = plotId
    await getUnimportedLocationsForTaskRunPlot(context, taskRunData)
    delete taskRunData.plotId
  }
  delete taskRunData.workflowPlots
}

async function getUnimportedLocationsForTaskRunPlot (context, taskRunData) {
  try {
    const unimportedLocationIds = await getLocationsToImportForTaskRunPlot(context, taskRunData)
    if (unimportedLocationIds) {
      await addItemForReplay(context, taskRunData)
    }
  } catch (err) {
    if (err instanceof TimeseriesStagingError) {
      // The workflow exists in neither or both of the coastal and fluvial CSVs. Add the plot to the list of items
      // to be replayed so that it is still processed by the ImportFromFews function (causing a TimeseriesStagingException
      // to be created).
      await addItemForReplay(context, taskRunData)
    } else {
      throw err
    }
  }
}

async function addItemForReplay (context, taskRunData) {
  // Check the item does not already exist in the replay array
  const itemFound = taskRunData.itemsEligibleForReplay.some(replayItem => replayItem.sourceId === taskRunData.plotId)
  if (!itemFound) {
    taskRunData.itemsEligibleForReplay.push({
      sourceId: taskRunData.plotId,
      sourceType: 'P'
    })
  } else {
    context.log.warn(`Plot: ${taskRunData.plotId}, has already been identified as eligible for replay.`)
  }
}
