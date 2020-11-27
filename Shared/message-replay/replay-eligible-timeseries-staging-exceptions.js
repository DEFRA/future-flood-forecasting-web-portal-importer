const { executePreparedStatementInTransaction } = require('../transaction-helper')
const sql = require('mssql')

const activeTimeseriesStagingExceptionsByCsvTypeQuery = `
  select
    tse.payload
  from
    fff_staging.v_active_timeseries_staging_exception tse
  where
    tse.csv_error = 1 and
    tse.csv_type = @csvType
`
const serviceConfigUpdatedQuery = `
  select
    tse.payload
  from
    fff_staging.v_active_timeseries_staging_exception tse
  where
    tse.csv_error = 0 and
    0 <> (
      select
        count(id)
      from
        fff_staging.workflow_refresh
    ) and
    @secondsSinceCsvRefreshed >= all (
      select
        datediff(second, refresh_time, getutcdate())
      from
        fff_staging.workflow_refresh
   )
 `
module.exports = async function (context, replayData) {
  await executePreparedStatementInTransaction(replayMessagesForCsvRelatedTimeseriesStagingExceptions, context, replayData.transaction, replayData)
  await executePreparedStatementInTransaction(replayMessagesForTimeseriesStagingExceptionsIfServiceConfigUpdateHasBeenProcessed, context, replayData.transaction)
}

async function replayMessagesForTimeseriesStagingExceptionsIfServiceConfigUpdateHasBeenProcessed (context, preparedStatement) {
  await preparedStatement.input('secondsSinceCsvRefreshed', sql.Int)
  await preparedStatement.prepare(serviceConfigUpdatedQuery)

  const parameters = {
    secondsSinceCsvRefreshed: process.env['SERVICE_CONFIG_UPDATE_DETECTION_LIMIT'] || 300
  }

  await replayMessagesForTimeseriesStagingExceptions(context, preparedStatement, parameters)
}

async function replayMessagesForCsvRelatedTimeseriesStagingExceptions (context, preparedStatement, replayData) {
  await preparedStatement.input('csvType', sql.NVarChar)
  await preparedStatement.prepare(activeTimeseriesStagingExceptionsByCsvTypeQuery)

  const parameters = {
    csvType: replayData.csvType
  }

  await replayMessagesForTimeseriesStagingExceptions(context, preparedStatement, parameters)
}

async function replayMessagesForTimeseriesStagingExceptions (context, preparedStatement, parameters) {
  const result = await preparedStatement.execute(parameters)

  for (const record of result.recordset) {
    context.bindings.importFromFews.push(JSON.parse(record.payload))
  }
}
