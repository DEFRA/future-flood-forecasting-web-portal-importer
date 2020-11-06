const { executePreparedStatementInTransaction } = require('../transaction-helper')
const sql = require('mssql')

module.exports = async function (context, replayData) {
  await executePreparedStatementInTransaction(getMessagesForCsvRelatedTimeseriesStagingExceptions, context, replayData.transaction, replayData)
}

async function getMessagesForCsvRelatedTimeseriesStagingExceptions (context, preparedStatement, replayData) {
  await preparedStatement.input('@csvType', sql.NVarChar)

  await preparedStatement.prepare(`
    select
      tse.payload
    from
      fff_staging.timeseries_staging_exception tse
    where
      tse.csv_error = 1 and
      tse.csv_type = @csvType
  `)

  const parameters = {
    csvType: replayData.csvType
  }

  const result = await preparedStatement.execute(parameters)

  for (const record of result.recordset) {
    replayData.coreForecastingEngineMessages.push(record.payload)
  }
}
