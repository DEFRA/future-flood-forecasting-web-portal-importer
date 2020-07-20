const sql = require('mssql')

module.exports = async function (context, preparedStatement, errorData) {
  await preparedStatement.input('sourceId', sql.NVarChar)
  await preparedStatement.input('sourceType', sql.NVarChar)
  await preparedStatement.input('csvError', sql.Bit)
  await preparedStatement.input('csvType', sql.NVarChar)
  await preparedStatement.input('description', sql.NVarChar)
  await preparedStatement.input('fewsParameters', sql.NVarChar)
  await preparedStatement.input('timeseriesHeaderId', sql.UniqueIdentifier)

  await preparedStatement.prepare(`
    insert into
      fff_staging.timeseries_staging_exception
        (source_id, source_type, csv_error, csv_type, fews_parameters, timeseries_header_id, description)
    values
     (@sourceId, @sourceType, @csvError, @csvType, @fewsParameters, @timeseriesHeaderId, @description)
  `)

  const parameters = {
    sourceId: errorData.sourceId,
    sourceType: errorData.sourceType,
    csvError: errorData.csvError,
    csvType: errorData.csvType,
    fewsParameters: errorData.fewsParameters,
    timeseriesHeaderId: errorData.timeseriesHeaderId,
    description: errorData.description
  }

  await preparedStatement.execute(parameters)
}
