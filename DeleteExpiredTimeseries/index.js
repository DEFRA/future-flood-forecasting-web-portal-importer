const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const deleteStagingExceptions = require('./deleteStagingExceptions')
const insertDataIntoTemp = require('./insertDataIntoTempTable')
const moment = require('moment')
const sql = require('mssql')
const dropTempTableSql = 'drop table if exists #deletion_job_temp;'

const deleteQueries = {
  reporting_timeseries_job: `
    delete r 
    from fff_reporting.timeseries_job r
    inner join #deletion_job_temp te
      on te.reporting_id = r.id
    `,
  staging_inactive_timeseries_staging_exception: `
    delete itse 
    from fff_staging.inactive_timeseries_staging_exception itse
    inner join #deletion_job_temp te
      on itse.timeseries_staging_exception_id = te.exceptions_id
    `,
  staging_timeseries_staging_exception: `
    delete tse 
    from fff_staging.timeseries_staging_exception tse
    inner join #deletion_job_temp te
      on te.exceptions_id = tse.id
    `,
  staging_timeseries: `
    delete t 
    from fff_staging.timeseries t
    inner join #deletion_job_temp te
      on te.timeseries_id = t.id
    `,
  staging_timeseries_header: `
    delete th 
    from fff_staging.timeseries_header th
    inner join #deletion_job_temp te
      on te.timeseries_header_id = th.id
    `
}

module.exports = async function (context, myTimer) {
  // current time
  const timeStamp = moment().format()

  if (myTimer.isPastDue) {
    context.log('JavaScript is running late!')
  }

  if (process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT) {
    // The read-committed isolation level allows reads, writes and deletes on table data whilst the delete is
    // running (the locks are released after reading, there are no modified objects in the query so no further locks should take place).
    // Read committed ensures only committed data is selected to delete. Read committed does not protect against Non-repeatable reads or Phantom reads,
    // however the higher isolation levels (given the nature of the queries in the transaction) do not justify the concurrency cost in this case.
    await doInTransaction({
      fn: removeExpiredTimeseries,
      context,
      errorMessage: 'The expired timeseries deletion has failed with the following error:',
      isolationLevel: sql.ISOLATION_LEVEL.READ_COMMITTED
    })
  } else {
    context.log.warn('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT needs setting before timeseries can be removed.')
    throw new Error('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT needs setting before timeseries can be removed.')
  }
  context.log.info('The DeleteExpiredTimeseries function ran!', timeStamp)
  // context.done() is not required as there is no output binding to be activated.
}

async function removeExpiredTimeseries (transaction, context) {
  const expirationDate = await setDeletionDate(context)

  await createTempTable(transaction, context)

  let deleteRowBatchSize
  process.env.TIMESERIES_DELETE_BATCH_SIZE ? deleteRowBatchSize = process.env.TIMESERIES_DELETE_BATCH_SIZE : deleteRowBatchSize = 1000

  await insertDataIntoTemp(context, transaction, expirationDate, deleteRowBatchSize)

  context.log.info('Data delete starting.')
  // The order of deletion is sensitive to referential integrity
  await executePreparedStatementInTransaction(deleteRecords, context, transaction, 'fff_reporting.timeseries_job', deleteQueries.reporting_timeseries_job)
  await executePreparedStatementInTransaction(deleteRecords, context, transaction, 'fff_staging.inactive_timeseries_staging_exception', deleteQueries.staging_inactive_timeseries_staging_exception)
  await executePreparedStatementInTransaction(deleteRecords, context, transaction, 'fff_staging.timeseries_staging_exception', deleteQueries.staging_timeseries_staging_exception)
  await executePreparedStatementInTransaction(deleteRecords, context, transaction, 'fff_staging.timeseries', deleteQueries.staging_timeseries)
  await executePreparedStatementInTransaction(deleteRecords, context, transaction, 'fff_staging.timeseries_header', deleteQueries.staging_timeseries_header)
  await deleteStagingExceptions(context, transaction, expirationDate, deleteRowBatchSize)
  await dropTempTable(context, transaction)
}

async function setDeletionDate (context) {
  let expirationDate
  const limit = parseInt(process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT)
  // Dates need to be specified as UTC using ISO 8601 date formatting manually to ensure portability between local and cloud environments.
  // Not using toUTCString() as toISOString() supports ms.
  if ((typeof limit !== 'undefined') && Number.isInteger(limit) && limit > 0) {
    // This check is required to prevent zero subtraction, the downstream effect would be the removal of all data prior to the current date.
    expirationDate = moment.utc().subtract(limit, 'hours').toDate().toISOString()
  } else {
    context.log.error('The limit must be an integer greater than 0.')
    throw new Error('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT must be an integer greater than 0.')
  }

  return [expirationDate]
}

async function createTempTable (transaction, context) {
  context.log.info('Building temp table')
  // Ensure a local temporary table exists to store deletion jobs.
  // Deletion of the local temporary table associated with the pooled database connection
  // is not managed by connection reset (see http://tediousjs.github.io/tedious/api-connection.html#function_reset)
  // as this appears to cause intermittent connection state problems.
  // Deletion of the local temporary table associated with the pooled database connection is performed manually.
  await new sql.Request(transaction).batch(`
    drop table if exists #deletion_job_temp;
    create table #deletion_job_temp
    (
      reporting_id uniqueidentifier,
      timeseries_id uniqueidentifier,
      timeseries_header_id uniqueidentifier not null,
      exceptions_id uniqueidentifier,
      import_time datetimeoffset,
    );
    create clustered index ix_deletion_job_temp_reporting_id
      on #deletion_job_temp (reporting_id);
    create index ix_deletion_job_temp_timeseries_id
      on #deletion_job_temp (timeseries_id);
    create index ix_deletion_job_temp_timeseries_header_id
      on #deletion_job_temp (timeseries_header_id);
    create index ix_deletion_job_temp_exceptions_id
      on #deletion_job_temp (exceptions_id);
  `)
}

async function deleteRecords (context, preparedStatement, tableName, deleteQuery) {
  await preparedStatement.prepare(deleteQuery + ';select @@rowcount as deleted')
  const result = await preparedStatement.execute()
  context.log.info(`The 'DeleteExpiredTimeseries' function has deleted ${result.recordset[0].deleted} rows from the ${tableName} table.`)
}

async function dropTempTable (context, transaction) {
  await new sql.Request(transaction).batch(dropTempTableSql)
}
