const moment = require('moment')
const axios = require('axios')
const sql = require('mssql')

module.exports = async function timeseriesRefresh (context, message, fluvialDisplayGroupWorkflowsResponse, workflowId, preparedStatement) {
  const displayGroupData = await getDisplayGroupData(context, message, fluvialDisplayGroupWorkflowsResponse, workflowId, preparedStatement)
  const timeSeriesDisplayGroupsData = await getTimeseriesDisplayGroups(displayGroupData)
  await loadTimeseriesDisplayGroups(context, timeSeriesDisplayGroupsData, preparedStatement)
}

async function getDisplayGroupData (context, message, fluvialDisplayGroupWorkflowsResponse, workflowId, preparedStatement) {
  const displayGroupData = {}

  for (const record of fluvialDisplayGroupWorkflowsResponse.recordset) {
    displayGroupData[record.plot_id] = record.location_ids
  }

  return displayGroupData
}

async function getTimeseriesDisplayGroups (displayGroupData) {
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

  const data = {
    startTime: startTime,
    endTime: endTime,
    timeseries: []
  }

  for (const key of Object.keys(displayGroupData)) {
    const plotId = `&plotId=${key}`
    const locationIds = `&locationIds=${displayGroupData[key].replace(/;/g, '&locationIds=')}`
    const fewsParameters = `${plotId}${locationIds}${fewsStartTime}${fewsEndTime}`

    // Get the timeseries display groups for the configured plot, locations and date range.
    const fewsPiEndpoint =
      `${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries/displaygroups?useDisplayUnits=false
        &showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON${fewsParameters}`

    const fewsResponse = await axios.get(fewsPiEndpoint)
    data.timeseries.push(JSON.stringify(fewsResponse.data))
  }
  return data
}

async function loadTimeseriesDisplayGroups (context, timeSeriesDisplayGroupsData, preparedStatement) {
  try {
    await preparedStatement.input('timeseries', sql.NVarChar)
    await preparedStatement.input('startTime', sql.DateTime2)
    await preparedStatement.input('endTime', sql.DateTime2)
    await preparedStatement.output('insertedId', sql.UniqueIdentifier)

    await preparedStatement.prepare(`
      insert into
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries (fews_data, start_time, end_time)
      output
        inserted.id
      values
       (@timeseries, @startTime, @endTime)
    `)

    context.bindings.stagedTimeseries = []

    for (const index in timeSeriesDisplayGroupsData.timeseries) {
      const parameters = {
        timeseries: timeSeriesDisplayGroupsData.timeseries[index],
        startTime: timeSeriesDisplayGroupsData.startTime,
        endTime: timeSeriesDisplayGroupsData.endTime
      }

      const result = await preparedStatement.execute(parameters)

      // Prepare to send a message containing the primary key of the inserted record.
      if (result.recordset && result.recordset[0] && result.recordset[0].id) {
        context.bindings.stagedTimeseries.push({
          id: result.recordset[0].id
        })
      }
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
