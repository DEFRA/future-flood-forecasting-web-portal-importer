const { executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const sql = require('mssql')

const queryRootSoft = `
-- ! the softdate query will need refactoring to accoutn for partial loading both in timeseries and timeseries_staging_exception
insert into #deletion_job_temp
  (reporting_id, timeseries_id, timeseries_header_id, import_time, exceptions_id)
-- linked rows in header-timeseries-reporting (not including rows in only header-timeseries or only header)
  select
    r.id as reporting_id,
    t.id as timeseries_id,
    h.id as header_id,
    h.import_time,
    e.id as exceptions_id
  from
    fff_reporting.timeseries_job r
    join fff_staging.timeseries t on t.id = r.timeseries_id
    join (
  select
      top(@deleteHeaderBatchSize)
      id,
      import_time
    from fff_staging.timeseries_header
    order by
      import_time
  ) h on t.timeseries_header_id = h.id
  -- for a soft date use a left join on tse to account for the records that exist in both timeseries and tse (this is pressuming none partial loading)
  left join fff_staging.timeseries_staging_exception e on e.timeseries_header_id = h.id
  where
  h.import_time < cast(@date as datetimeoffset)
  and r.job_status = @completeStatus
  `

const queryRootHard = `
insert into #deletion_job_temp
  (reporting_id, timeseries_id, timeseries_header_id, import_time, exceptions_id)
-- linked rows in header-timeseries-reporting (not including rows in only header-timeseries or only header)
  select
    r.id as reporting_id,
    t.id as timeseries_id,
    h.id as header_id,
    h.import_time,
    null as exceptions_id
  from
    fff_reporting.timeseries_job r
    join fff_staging.timeseries t on t.id = r.timeseries_id
    join (
  select
      top(@deleteHeaderBatchSize)
      id,
      import_time
    from fff_staging.timeseries_header
    order by
      import_time
  ) h on t.timeseries_header_id = h.id
  where
  h.import_time < cast(@date as datetimeoffset)
union
  -- linked rows in header-exceptions (not including rows in only header)
  select
    null as reporting_id,
    null as timeseries_id,
    h.id as header_id,
    h.import_time,
    e.id as exceptions_id
  from
    fff_staging.timeseries_staging_exception e
    join (
    select
      top(@deleteHeaderBatchSize)
      id,
      import_time
    from fff_staging.timeseries_header
    order by
      import_time
  ) h on e.timeseries_header_id = h.id
  where
   h.import_time < cast(@date as datetimeoffset)
union
  -- linked rows only in header-timeseries
  select
    null as reporting_id,
    t.id as timeseries_id,
    h.id as header_id,
    h.import_time,
    null as exceptions_id
  from fff_staging.timeseries t
    join (
  select
      top(@deleteHeaderBatchSize)
      id,
      import_time
    from fff_staging.timeseries_header
    order by
    import_time
) h on t.timeseries_header_id = h.id
  where
h.import_time < cast(@date as datetimeoffset)
union
  -- rows only in header
  select top(@deleteHeaderBatchSize)
    null as reporting_id,
    null as timeseries_id,
    h.id as header_id,
    h.import_time,
    null as exceptions_id
  from fff_staging.timeseries_header h
  where
  h.import_time < cast(@date as datetimeoffset)`

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
  if (isSoftDate) {
    // the softdate query will need refactoring to account for partial loading both in timeseries and timeseries_staging_exception
    await preparedStatement.prepare(queryRootSoft)
  } else {
    await preparedStatement.prepare(queryRootHard)
  }
  const parameters = {
    date: date,
    completeStatus: FME_COMPLETE_JOB_STATUS,
    deleteHeaderBatchSize: deleteHeaderRowBatchSize
  }

  await preparedStatement.execute(parameters)
}
