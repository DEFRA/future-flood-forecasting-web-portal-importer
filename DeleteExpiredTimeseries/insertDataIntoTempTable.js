import { executePreparedStatementInTransaction } from '../Shared/transaction-helper.js'
import sql from 'mssql'

const collectExpiredRecordsQuery = `
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

export default async function (context, transaction, date, deleteHeaderRowBatchSize) {
  await executePreparedStatementInTransaction(insertDataIntoTemp, context, transaction, date, deleteHeaderRowBatchSize)
}

async function insertDataIntoTemp (context, preparedStatement, date, deleteHeaderRowBatchSize) {
  context.log.info('Loading expired data into temp table')

  await preparedStatement.input('date', sql.DateTimeOffset)
  await preparedStatement.input('deleteHeaderRowBatchSize', sql.Int)
  await preparedStatement.prepare(collectExpiredRecordsQuery)
  const parameters = {
    date,
    deleteHeaderRowBatchSize
  }

  await preparedStatement.execute(parameters)
}
