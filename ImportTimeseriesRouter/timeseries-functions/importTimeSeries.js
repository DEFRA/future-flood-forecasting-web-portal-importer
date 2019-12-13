const moment = require('moment')
const axios = require('axios')
const sql = require('mssql')

module.exports = async function timeseriesRefresh (context, message, fluvialNonDisplayGroupWorkflowsResponse, workflowId, preparedStatement) {
  const nonDisplayGroupData = await getNonDisplayGroupData(context, message, fluvialNonDisplayGroupWorkflowsResponse, workflowId, preparedStatement)
  const timeSeriesNonDisplayGroupsData = await getTimeseriesNonDisplayGroups(nonDisplayGroupData)
  await loadTimeseriesNonDisplayGroups(context, timeSeriesNonDisplayGroupsData, preparedStatement)
}

async function getNonDisplayGroupData (context, message, fluvialNonDisplayGroupWorkflowsResponse, workflowId, preparedStatement) {
  const nonDisplayGroupData = []

  for (const record of fluvialNonDisplayGroupWorkflowsResponse.recordset) {
    nonDisplayGroupData.push(record.filter_id)
  }

  return nonDisplayGroupData
}

async function getTimeseriesNonDisplayGroups (nonDisplayGroupData) {
  const now = moment.utc()
  // Retrieve data from the last two days to the next five days by default.
  // This time period can be overridden by the two environment variables
  // FEWS_START_TIME_OFFSET_HOURS and FEWS_END_TIME_OFFSET_HOURS.
  const startTimeOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 48
  const endTimeOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120
  const startTime = moment(now).subtract(startTimeOffsetHours, 'hours').toISOString()
  const endTime = moment(now).add(endTimeOffsetHours, 'hours').toISOString()

  // The database in which data is loaded requires fractional seconds to be included in dates. By contrast
  // the REST interface of the core forecasting engine requires fractional seconds to be excluded from dates.
  const fewsStartTime = `&startTime=${startTime.substring(0, 19)}Z`
  const fewsEndTime = `&endTime=${endTime.substring(0, 19)}Z`

  // need from workflow message?
  // "startDate":{"date":"2019-12-01","time":"11:15:00"},"endDate":{"date":"2019-12-08","time":"11:30:00"},

  const data = {
    startTime: startTime,
    endTime: endTime,
    timeseries: []
  }

  for (const value of nonDisplayGroupData) {
    const filterId = `&filterId=${value}`
    const fewsParameters = `${filterId}${fewsStartTime}${fewsEndTime}`

    // Get the timeseries display groups for the configured plot, locations and date range.
    const fewsPiEndpoint = `${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries?useDisplayUnits=false&showThresholds=true&showProducts=false&omitMissing=true&onlyHeaders=false&showEnsembleMemberIds=false&documentVersion=1.26&documentFormat=PI_JSON&forecastCount=1${fewsParameters}`
    const fewsResponse = await axios.get(fewsPiEndpoint)
    data.timeseries.push(JSON.stringify(fewsResponse.data))
  }
  return data
}

async function loadTimeseriesNonDisplayGroups (context, timeSeriesNonDisplayGroupsData, preparedStatement) {
  try {
    await preparedStatement.input('timeseries', sql.NVarChar)
    await preparedStatement.input('startTime', sql.DateTime2)
    await preparedStatement.input('endTime', sql.DateTime2)

    await preparedStatement.prepare(`
      insert into
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries (fews_data, start_time, end_time)
      output
        inserted.id
      values
       (@timeseries, @startTime, @endTime)
    `)

    for (const index in timeSeriesNonDisplayGroupsData.timeseries) {
      const parameters = {
        timeseries: timeSeriesNonDisplayGroupsData.timeseries[index], // .substring(1, timeSeriesNonDisplayGroupsData.timeseries[index].length - 1),
        startTime: timeSeriesNonDisplayGroupsData.startTime,
        endTime: timeSeriesNonDisplayGroupsData.endTime
      }

      await preparedStatement.execute(parameters)
      // TO DO - Send a message containing the primary key of the new record to a queue.
    }
  } finally {
    try {
      if (preparedStatement) {
        await preparedStatement.unprepare()
      }
    } catch (err) {
      context.log(err)
    }
  }
}
