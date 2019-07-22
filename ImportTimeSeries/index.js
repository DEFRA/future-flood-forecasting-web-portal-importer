module.exports = async function (context, message) {
  const moment = require('moment')
  const axios = require('axios')
  const sql = require('mssql')
  const uuidv4 = require('uuid/v4')

  // This function is triggered via a queue message drop
  context.log('JavaScript queue trigger function processed work item', message)
  context.log(context.bindingData)

  let pool
  let preparedStatement
  try {
    // Get the timeseries for the previous day for the location identified in the message from the FEWS PI server.
    const now = moment.utc()
    const startTime = moment(now).utc().subtract(24, 'hours').toISOString()
    const endTime = now.toISOString()
    const fewsStartTime = `&startTime=${startTime.substring(0, 19)}Z`
    const fewsEndTime = `&endTime=${endTime.substring(0, 19)}Z`
    const fewsParameters = `&locationIds=${message}${fewsStartTime}${fewsEndTime}`
    const fewsPiEndpoint = `${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries?locationIds=${message}&useDisplayUnits=false&showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON${fewsParameters}`
    const fewsResponse = await axios.get(fewsPiEndpoint)
    const timeseries = JSON.stringify(fewsResponse.data)

    // Insert the timeseries into the staging database
    pool = await sql.connect(process.env['SQLDB_CONNECTION_STRING'])
    preparedStatement = new sql.PreparedStatement(pool)
    await preparedStatement.input('id', sql.UniqueIdentifier)
    await preparedStatement.input('timeseries', sql.NVarChar)
    await preparedStatement.input('startTime', sql.DateTime2)
    await preparedStatement.input('endTime', sql.DateTime2)
    await preparedStatement.prepare(`INSERT INTO ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries (id, fews_data, start_time, end_time) values (@id,  @timeseries, @startTime, @endTime)`)
    const parameters = {
      id: uuidv4(),
      timeseries: timeseries,
      startTime: startTime,
      endTime: endTime
    }
    await preparedStatement.execute(parameters)
  } catch (err) {
    context.log.error(err)
    throw err
  } finally {
    try {
      if (preparedStatement) {
        await preparedStatement.unprepare()
      }
    } catch (err) { }
    try {
      if (pool) {
        await pool.close()
      }
    } catch (err) { }
    try {
      if (sql) {
        await sql.close()
      }
    } catch (err) { }
  }
  sql.on('error', err => {
    context.log.error(err)
    throw err
  })
}
