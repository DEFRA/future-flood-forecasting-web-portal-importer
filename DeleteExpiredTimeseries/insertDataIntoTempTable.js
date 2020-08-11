const { executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const sql = require('mssql')

const queryRoot = `
  insert into #deletion_job_temp (reporting_id, timeseries_id, timeseries_header_id)
    select
      r.id,
      r.timeseries_id,
      t.timeseries_header_id
    from
      fff_reporting.timeseries_job r
      join fff_staging.timeseries t on t.id = r.timeseries_id
      join (
        select
          top(@deleteHeaderBatchSize) id,
          import_time from fff_staging.timeseries_header
        order by
          import_time
      ) h on t.timeseries_header_id = h.id
    where
      h.import_time < cast(@date as DateTimeOffset)
`

module.exports = async function (context, transaction, date, isSoftDate) {
  await executePreparedStatementInTransaction(insertDataIntoTemp, context, transaction, date, isSoftDate)
}

async function insertDataIntoTemp (context, preparedStatement, date, isSoftDate) {
  context.log.info(`Loading ${isSoftDate ? 'Soft' : 'Hard'} data into temp table`)
  const FME_COMPLETE_JOB_STATUS = 6
  let deleteHeaderRowBatchSize
  process.env['TIMESERIES_DELETE_BATCH_SIZE'] ? deleteHeaderRowBatchSize = process.env['TIMESERIES_DELETE_BATCH_SIZE'] : deleteHeaderRowBatchSize = 1000

  await preparedStatement.input('date', sql.DateTimeOffset)
  await preparedStatement.input('completeStatus', sql.Int)
  await preparedStatement.input('deleteHeaderBatchSize', sql.Int)
  await preparedStatement.prepare(`${queryRoot} ${isSoftDate ? 'and r.job_status = @completeStatus' : ''}`)

  const parameters = {
    date: date,
    completeStatus: FME_COMPLETE_JOB_STATUS,
    deleteHeaderBatchSize: deleteHeaderRowBatchSize
  }

  await preparedStatement.execute(parameters)
}
