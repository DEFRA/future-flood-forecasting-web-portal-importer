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

const serviceConfigurationUpdateCompletedMessage = `{
  "input": "notify"
}`

module.exports = {
  processServiceConfigurationUpdateForAllCsvDataIfRequired: async function (context, preparedStatement) {
    // Determine if a service configuration update for all CSV data has occurred.
    const parameters = {
      secondsSinceCsvRefreshed: process.env.SERVICE_CONFIG_UPDATE_DETECTION_LIMIT || 300
    }

    await prepareServiceConfigurationUpdateDetectionQuery(context, preparedStatement, false)
    const result = preparedStatement.execute(parameters)

    if (result && result.recordset && result.recordset[0]) {
      await prepareToEnableCoreEngineTaskRunProcessingIfNeeded(context)
    }
  },
  prepareServiceConfigurationUpdateDetectionQueryForWorkflowCsvData: async function (context, preparedStatement) {
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

async function prepareToEnableCoreEngineTaskRunProcessingIfNeeded (context) {
  // A service configuration update for all CSV data has occurred.
  // If the ProcessFewsEventCode or ImportFromFews function is disabled (for example, during a deployment or failover scenario)
  // place a message on the fews-service-configuration-update-completed-queue so that the function(s) can be enabled.
  const messageToLog = 'A full service configuration update has been completed'
  if (process.env['AzureWebJobs.ProcessFewsEventCode.Disabled'] || process.env['AzureWebJobs.ImportFromFews.Disabled']) {
    context.log(`${messageToLog} - preparing to send notification`)
    context.bindings.serviceConfigurationUpdateCompleted = [serviceConfigurationUpdateCompletedMessage]
  } else {
    context.log(messageToLog)
  }
}
