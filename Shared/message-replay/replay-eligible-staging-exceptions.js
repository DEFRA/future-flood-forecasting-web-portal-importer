const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = async function (context, replayData) {
  await executePreparedStatementInTransaction(getCoreForecastingEngineMessagesForKnownWorkflows, context, replayData.transaction, replayData)
}

async function getCoreForecastingEngineMessagesForKnownWorkflows (context, preparedStatement, replayData) {
  await preparedStatement.input('csvType', sql.NVarChar)

  // Note that table locks are held on each table used by the workflow view for the duration of the transaction to
  // guard against a workflow table refresh during processing.
  await preparedStatement.prepare(`
    select distinct
      se.payload
    from
      fff_staging.staging_exception se
      inner join fff_staging.v_workflow vw
        on se.workflow_id = vw.workflow_id
    where
      vw.csv_type = @csvType and
      se.source_function = 'P' and
      se.description like 'Missing PI Server input data for%'
  `)

  const parameters = {
    csvType: replayData.csvType
  }

  const result = await preparedStatement.execute(parameters)

  for (const record of result.recordset) {
    context.bindings.processFewsEventCode.push(record.payload)
  }
}
