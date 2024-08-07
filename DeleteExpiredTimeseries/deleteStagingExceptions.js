import { executePreparedStatementInTransaction } from '../Shared/transaction-helper.js'
import sql from 'mssql'

const deleteStagingExceptionsQuery = `
-- staging exceptions to be deleted
with
  dsecte
  as
  (
    select 
      top (@deleteRowBatchSize)*
    from 
      fff_staging.staging_exception
    where
      exception_time < cast(@expiryDate as datetimeoffset)
      order by exception_time
  )
delete 
  from 
    dsecte
select 
  @@rowcount as deleted`

const deleteInactiveStagingExceptionsQuery = `
-- inactive staging exceptions to be deleted
delete 
  ise
from 
  fff_staging.inactive_staging_exception ise
join (
  select
    top(@deleteRowBatchSize)
    id
  from
    fff_staging.staging_exception se
  where
    se.exception_time < cast(@expiryDate as datetimeoffset)
  order by
    se.exception_time
  ) se on ise.staging_exception_id = se.id
select 
  @@rowcount as deleted`

export default async function (context, transaction, expiryDate, deleteRowBatchSize) {
  const deleteStagingExceptionData = {
    expiryDate,
    deleteRowBatchSize,
    deleteQuery: deleteStagingExceptionsQuery,
    table: 'StagingException'
  }
  const deleteInactiveStagingExceptionData = {
    expiryDate,
    deleteRowBatchSize,
    deleteQuery: deleteInactiveStagingExceptionsQuery,
    table: 'InactiveStagingException'
  }
  await executePreparedStatementInTransaction(deleteExceptions, context, transaction, deleteInactiveStagingExceptionData)
  await executePreparedStatementInTransaction(deleteExceptions, context, transaction, deleteStagingExceptionData)
}

async function deleteExceptions (context, preparedStatement, deleteContext) {
  await preparedStatement.input('expiryDate', sql.DateTimeOffset)
  await preparedStatement.input('deleteRowBatchSize', sql.Int)
  await preparedStatement.prepare(deleteContext.deleteQuery)
  const parameters = {
    expiryDate: deleteContext.expiryDate,
    deleteRowBatchSize: deleteContext.deleteRowBatchSize
  }

  const result = await preparedStatement.execute(parameters)
  context.log.info(`The 'DeleteExpiredTimeseries' function has deleted ${result.recordset[0].deleted} rows from the ${deleteContext.table} table.`)
}
