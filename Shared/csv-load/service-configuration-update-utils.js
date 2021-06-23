const sql = require('mssql')

const startOfServiceConfigurationUpdateDetectionQueryWhenCheckingWorkflowCsvTablesOnly = `
  select
    tse.payload as message
  from
    fff_staging.v_active_timeseries_staging_exception tse
  where
    tse.csv_error = 0 and  
`

const startOfServiceConfigurationUpdateDetectionQueryWhenCheckingAllCsvTables = `
  select
    1
  from
`

module.exports = {
  prepareServiceConfigurationUpdateDetectionQueryForAllCsvTables: async function (context, preparedStatement) {
    return await prepareServiceConfigurationUpdateDetectionQuery(context, preparedStatement, false)
  },
  prepareServiceConfigurationUpdateDetectionQueryForWorkflowCsvTables: async function (context, preparedStatement) {
    return await prepareServiceConfigurationUpdateDetectionQuery(context, preparedStatement, true)
  }
}

async function prepareServiceConfigurationUpdateDetectionQuery (context, preparedStatement, checkWorkflowCsvTablesOnly) {
  const detectionSource = `fff_staging.${checkWorkflowCsvTablesOnly ? 'workflow_refresh' : 'v_csv_refresh'}`
  const startOfQuery =
    checkWorkflowCsvTablesOnly
      ? startOfServiceConfigurationUpdateDetectionQueryWhenCheckingWorkflowCsvTablesOnly
      : startOfServiceConfigurationUpdateDetectionQueryWhenCheckingAllCsvTables

  const serviceConfigurationUpdateDetectionQuery = `
    ${startOfQuery}  
      0 <> (
        select
          count(id)
        from
          ${detectionSource}
      ) and
      @secondsSinceCsvRefreshed >= all (
        select
          datediff(second, refresh_time, getutcdate())
        from
          ${detectionSource}
      )
    `
  await preparedStatement.input('secondsSinceCsvRefreshed', sql.Int)
  await preparedStatement.prepare(serviceConfigurationUpdateDetectionQuery)
}
