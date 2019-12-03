const moment = require('moment')
const axios = require('axios')
const sql = require('mssql')
const createStagingException = require('../../Shared/create-staging-exception')

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

  if (Object.keys(displayGroupData).length === 0) {
    // If no display group data is available the message is not replayable
    // without intervention so create a staging exception.
    await createStagingException(context, message, `Missing display_group data for ${workflowId}`, preparedStatement)
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

    await preparedStatement.prepare(`
      insert into
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries (fews_data, start_time, end_time)
      output
        inserted.id
      values
       (@timeseries, @startTime, @endTime)
    `)

    for (const index in timeSeriesDisplayGroupsData.timeseries) {
      const parameters = {
        timeseries: timeSeriesDisplayGroupsData.timeseries[index], // .substring(1, timeSeriesDisplayGroupsData.timeseries[index].length - 1),
        startTime: timeSeriesDisplayGroupsData.startTime,
        endTime: timeSeriesDisplayGroupsData.endTime
      }

      await preparedStatement.execute(parameters)
      // TO DO - Send a message containing the primary key of the new record to a queue.
    }
  } catch (err) {
    context.log.error(err)
    throw err
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
