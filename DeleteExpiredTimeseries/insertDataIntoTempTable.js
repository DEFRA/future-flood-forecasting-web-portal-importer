const sql = require('mssql')

module.exports = async function insertDataIntoTemp (context, preparedStatement, date, isSoftDate) {
  context.log.info(`Loading ${isSoftDate ? 'Soft' : 'Hard'} data into temp table`)
  const FME_COMPLETE_JOB_STATUS = 6

  await preparedStatement.input('date', sql.DateTimeOffset)
  await preparedStatement.input('completeStatus', sql.Int)

  const query = `insert into #deletion_job_temp (reporting_id, timeseries_id, timeseries_header_id)
      select r.id, r.timeseries_id, t.timeseries_header_id
      from [${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}].timeseries_job r
        join [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].timeseries t on t.id = r.timeseries_id
        join [${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}].timeseries_header h on t.timeseries_header_id = h.id
      where
        h.import_time < cast(@date as DateTimeOffset) ${isSoftDate ? 'and r.job_status = @completeStatus' : ''}`

  await preparedStatement.prepare(query)

  const parameters = {
    date: date,
    completeStatus: FME_COMPLETE_JOB_STATUS
  }

  await preparedStatement.execute(parameters)
}
