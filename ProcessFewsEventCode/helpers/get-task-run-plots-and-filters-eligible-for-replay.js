const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const getLocationsToImportForTaskRunPlot = require('../../Shared/timeseries-functions/get-locations-to-import-for-task-run-plot')
const sql = require('mssql')

// Note that table locks are held on each table used by the workflow view for the duration of the transaction to
// guard against a workflow table refresh during processing.
const taskRunPlotsAndFiltersWithActiveTimeseriesStagingExceptionsQuery = `
  select 
    source_id,
    source_type
  from
    fff_staging.v_workflow
  where
    workflow_id = @workflowId
  intersect
  select
    tse.source_id,
    tse.source_type
  from
    fff_staging.timeseries_header th,
    fff_staging.v_active_timeseries_staging_exception tse
  where
    th.id = tse.timeseries_header_id and
    th.task_run_id = @taskRunId and
    (
      tse.csv_error = 0 or
      (
        tse.exception_time < (
          select
            wr.refresh_time
          from
            fff_staging.workflow_refresh wr
          where
            tse.csv_type = wr.csv_type
        )    
      )    
    )    
`
const workflowPlotsQuery = `
  select
    source_id as plot_id
  from
    fff_staging.v_workflow
  where
    workflow_id = @workflowId and
    source_type = 'P'
`
module.exports = async function (context, taskRunData) {
  await executePreparedStatementInTransaction(getTaskRunPlotsAndFiltersWithActiveTimeseriesStagingExceptions, context, taskRunData.transaction, taskRunData)
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
  const unimportedLocationIds = await getLocationsToImportForTaskRunPlot(context, taskRunData)

  if (unimportedLocationIds) {
    // taskRunData.itemsEligibleForReplay.push({
    //   sourceId: taskRunData.plotId,
    //   sourceType: 'P'
    // })
  }
}
