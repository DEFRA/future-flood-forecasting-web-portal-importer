module.exports = async function (context, importTimeSeriesTimer) {
  const moment = require('moment')
  const axios = require('axios')
  const sql = require('mssql')
  const uuidv4 = require('uuid/v4')
  const plotId = process.env['FEWS_PLOT_ID'] ? '&plotId=' + process.env['FEWS_PLOT_ID'] : ''
  const locationIds = process.env['FEWS_LOCATION_IDS'] ? '&locationIds=' + process.env['FEWS_LOCATION_IDS'].replace(/;/g, '&locationIds=') : ''

  if (importTimeSeriesTimer.IsPastDue) {
    context.log('JavaScript is running late!')
  }

  // This function is triggered via a timer configured in function json

  let pool
  let insertPreparedStatement
  let latestLoadEndDateRequest
  try {
    // Base the import date range on the dates for the previous import (if any).
    pool = await sql.connect(process.env['SQLDB_CONNECTION_STRING'])
    latestLoadEndDateRequest = new sql.Request(pool)
    const latestLoadEndDateResponse = await latestLoadEndDateRequest.query(`select max(end_time) as latest_end_time from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
    const latestEndTime = latestLoadEndDateResponse.recordset[0].latest_end_time
    const now = moment.utc()
    const newStartTime = latestEndTime ? moment.utc(latestEndTime).subtract(process.env['FEWS_LOAD_HISTORY_HOURS'], 'hours').toISOString() : moment(now).subtract(process.env['FEWS_INITIAL_LOAD_HISTORY_HOURS'], 'hours').toISOString()
    const newEndTime = moment(now).add(120, 'hours').toISOString()
    const fewsStartTime = `&startTime=${newStartTime.substring(0, 19)}Z`
    const fewsEndTime = `&endTime=${newEndTime.substring(0, 19)}Z`
    const fewsParameters = `${plotId}${locationIds}${fewsStartTime}${fewsEndTime}`

    // Get the timeseries display groups for the configured plot, locations and date range.
    const fewsPiEndpoint = `${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries/displaygroups?useDisplayUnits=false&showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON${fewsParameters}`
    const fewsResponse = await axios.get(fewsPiEndpoint)
    const timeseries = JSON.stringify(fewsResponse.data)

    // Insert the timeseries into the staging database
    insertPreparedStatement = new sql.PreparedStatement(pool)

    await insertPreparedStatement.input('id', sql.UniqueIdentifier)
    await insertPreparedStatement.input('timeseries', sql.NVarChar)
    await insertPreparedStatement.input('startTime', sql.DateTime2)
    await insertPreparedStatement.input('endTime', sql.DateTime2)
    await insertPreparedStatement.prepare(`insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries (id, fews_data, start_time, end_time) values (@id,  @timeseries, @startTime, @endTime)`)
    const parameters = {
      id: uuidv4(),
      timeseries: timeseries,
      startTime: newStartTime,
      endTime: newEndTime
    }
    await insertPreparedStatement.execute(parameters)
  } catch (err) {
    context.log.error(err)
    throw err
  } finally {
    try {
      if (insertPreparedStatement) {
        await insertPreparedStatement.unprepare()
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
  // done() not requried as the async function returns the desired result, there is no output binding to be activated.
  // context.done()
}
