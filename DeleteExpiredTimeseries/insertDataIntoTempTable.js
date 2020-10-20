const { executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const sql = require('mssql')

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
      top(@deleteHeaderRowBatchSize)
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
      top(@deleteHeaderRowBatchSize)
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
      top(@deleteHeaderRowBatchSize)
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
  select top(@deleteHeaderRowBatchSize)
    null as reporting_id,
    null as timeseries_id,
    h.id as header_id,
    h.import_time,
    null as exceptions_id
  from fff_staging.timeseries_header h
  where
  h.import_time < cast(@date as datetimeoffset)`

module.exports = async function (context, transaction, date, isSoftDate, deleteHeaderRowBatchSize) {
  await executePreparedStatementInTransaction(insertDataIntoTemp, context, transaction, date, isSoftDate, deleteHeaderRowBatchSize)
}

async function insertDataIntoTemp (context, preparedStatement, date, isSoftDate, deleteHeaderRowBatchSize) {
  context.log.info(`Loading ${isSoftDate ? 'Soft' : 'Hard'} data into temp table`)
  const FME_COMPLETE_JOB_STATUS = 6

  await preparedStatement.input('date', sql.DateTimeOffset)
  await preparedStatement.input('completeStatus', sql.Int)
  await preparedStatement.input('deleteHeaderRowBatchSize', sql.Int)
  if (isSoftDate) {
    // due to the introduction of partial loading soft limit deletes are currently inactive and pending refactoring
    const queryRootSoft = null
    await preparedStatement.prepare(queryRootSoft)
  } else {
    await preparedStatement.prepare(queryRootHard)
  }
  const parameters = {
    date,
    completeStatus: FME_COMPLETE_JOB_STATUS,
    deleteHeaderRowBatchSize
  }

  await preparedStatement.execute(parameters)
}
