import sql from 'mssql'
import { executePreparedStatementInTransaction } from '../transaction-helper.js'

const deactivateBySourceFunctionAndTaskRunIdQuery = `
  insert into
    fff_staging.inactive_staging_exception (staging_exception_id)
  select
    se.id
  from
    fff_staging.staging_exception se
  where
  task_run_id = @taskRunId and
  coalesce(
    source_function,
    convert(nvarchar(1), case
      when payload like '%description%' then 'P'
      when payload like '%taskRunId%' then 'I'
      end
    )
  ) = @sourceFunction and
  not exists
    (
      select
        1
      from
        fff_staging.inactive_staging_exception ise
      where
        ise.staging_exception_id = se.id
    )
`

// Note that table locks are held on each table used by the workflow view for the duration of the transaction to
// guard against a workflow table refresh during processing.
const deactivateTimeseriesStagingExceptionsForNonExistentTaskRunPlotsAndFiltersQuery = `
  insert into
    fff_staging.inactive_timeseries_staging_exception (timeseries_staging_exception_id)
  select
    tse.id
  from
    fff_staging.timeseries_staging_exception tse
    inner join fff_staging.timeseries_header th on th.id = tse.timeseries_header_id
    inner join
    -- Remove the set of plots/filters in the current workflow CSVs from the set of plots/filters
    -- linked to active TIMESERIES_STAGING_EXCEPTIONS.
    -- Any remaining plots/filters have been removed from the current workflow CSVs and
    -- associated active TIMESERIES_STAGING_EXCEPTIONS can be deactivated accordingly.
    -- This scenario will happen when plots/filter ID typos are corrected.
    (
      select
        source_id,
        source_type
      from
        fff_staging.v_active_timeseries_staging_exception tse,
        fff_staging.timeseries_header th
      where
        th.id = tse.timeseries_header_id and
        th.task_run_id = @taskRunId
      except
      (
        select
          source_id,
          source_type
        from
          fff_staging.v_workflow
        where
          workflow_id = @workflowId            
      )
    ) dtse -- Timeseries staging exceptions to be deactivated 
    on tse.source_id = dtse.source_id and
       tse.source_type = dtse.source_type
  where
    not exists
      (
        select
          1
        from
          fff_staging.inactive_timeseries_staging_exception itse
        where
          itse.timeseries_staging_exception_id = tse.id
      )
`

export const deactivateStagingExceptionBySourceFunctionAndTaskRunId = async function (context, stagingExceptionData) {
  await buildConfigurationAndPerformQuery(context, stagingExceptionData, deactivateBySourceFunctionAndTaskRunIdQuery, 'sourceFunction')
}

export const deactivateTimeseriesStagingExceptionsForNonExistentTaskRunPlotsAndFilters = async function (context, taskRunData) {
  await buildConfigurationAndPerformQuery(context, taskRunData, deactivateTimeseriesStagingExceptionsForNonExistentTaskRunPlotsAndFiltersQuery, 'workflowId')
}

async function buildConfigurationAndPerformQuery (context, data, query, requiredParameterName) {
  const config = {
    data,
    parameters: [
      {
        name: requiredParameterName,
        type: sql.NVarChar,
        value: data[requiredParameterName]
      }
    ],
    query
  }
  const transaction = data.transaction

  addTaskRunIdParameterConfig(context, config)
  await executePreparedStatementInTransaction(performQuery, context, transaction, config)
}

async function performQuery (context, preparedStatement, config) {
  const parameterValues = {}

  for (const parameter of config.parameters) {
    await preparedStatement.input(parameter.name, parameter.type)
    parameterValues[parameter.name] = parameter.value
  }

  await preparedStatement.prepare(config.query)
  await preparedStatement.execute(parameterValues)
}

function addTaskRunIdParameterConfig (context, config) {
  const taskRunIdParameterConfig = {
    name: 'taskRunId',
    type: sql.NVarChar,
    value: config.data.taskRunId
  }

  config.parameters.push(taskRunIdParameterConfig)
}
