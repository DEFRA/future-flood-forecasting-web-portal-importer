const { executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const sql = require('mssql')

const query = `
-- inactive staging exceptions to be deleted
delete ise
  from fff_staging.inactive_staging_exception ise
  join (
    select
    top(@deleteRowBatchSize)
    id
  from
    fff_staging.staging_exception se
  where 
    se.exception_time < cast(@date as datetimeoffset)
  order by
    se.exception_time
  ) se on ise.staging_exception_id = se.id

-- staging exceptions to be deleted
with
  deletesecte
  as
  (
    select top (@deleteRowBatchSize)
      *
    from fff_staging.staging_exception
    where 
    exception_time < cast(@date as datetimeoffset)
    order by exception_time
  )
delete from deletesecte
`

module.exports = async function (context, transaction, date) {
  await executePreparedStatementInTransaction(deleteStagingExceptions, context, transaction, date)
}

async function deleteStagingExceptions (context, preparedStatement, date) {
  context.log.info(`Deleting data for the `)
  let deleteRowBatchSize
  process.env['TIMESERIES_DELETE_BATCH_SIZE'] ? deleteRowBatchSize = process.env['TIMESERIES_DELETE_BATCH_SIZE'] : deleteRowBatchSize = 1000
  await preparedStatement.input('date', sql.DateTimeOffset)
  await preparedStatement.input('deleteBatchSize', sql.Int)
  await preparedStatement.prepare(query)
  const parameters = {
    date,
    deleteHeaderBatchSize: deleteRowBatchSize
  }
  await preparedStatement.execute(parameters)
}
