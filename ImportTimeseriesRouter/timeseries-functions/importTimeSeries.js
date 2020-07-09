const axios = require('axios')
const moment = require('moment')
const { gzip } = require('../../Shared/utils')
const getLatestEndTime = require('../helpers/get-latest-task-run-end-time')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')

const EXTERNAL_HISTORICAL = 'external_historical'
const EXTERNAL_FORECAST = 'external_forecasting'
const SIMULATED_FORECASTING = 'simulated_forecasting'

module.exports = async function getTimeseries (context, routeData) {
  const nonDisplayGroupData = await getNonDisplayGroupData(routeData.nonDisplayGroupWorkflowsResponse)
  const timeseries = await getTimeseriesInternal(context, nonDisplayGroupData, routeData)
  return timeseries
}

async function getNonDisplayGroupData (nonDisplayGroupWorkflowsResponse) {
  // Get the filter identifiers needed to retrieve timeseries from the REST
  // interface of the core forecasting engine.
  const nonDisplayGroupData = []

  for (const record of nonDisplayGroupWorkflowsResponse.recordset) {
    nonDisplayGroupData.push({
      filterId: record.filter_id,
      approvalRequired: record.approved,
      startTimeOffset: record.start_time_offset_hours,
      endTimeOffset: record.end_time_offset_hours,
      timeseriesType: record.timeseries_type
    })
  }

  return nonDisplayGroupData
}

async function getTimeseriesInternal (context, nonDisplayGroupData, routeData) {
  // The database in which data is loaded requires fractional seconds to be included in dates. By contrast
  // the REST interface of the core forecasting engine requires fractional seconds to be excluded from dates.

  await executePreparedStatementInTransaction(getLatestEndTime, context, routeData.transaction, routeData)

  // Retrieval of timeseries associated with a single task run of a non-display group workflow needs to be based on the time
  // at which the timeseries were created in the core engine. To ensure timeseries edited manually since the previous task run
  // are also retrieved, timeseries created between the end of the previous task run and the end of the current task run of
  // the workflow are retrieved. If this is the first task run of the workflow, timeseries created during the current task run
  // of the workflow are retrieved.
  // To try and prevent additional older timeseries created by core engine amalgamation being returned, queries to the core
  // engine PI Server also restrict returned timeseries to those associated with a time period designed to exclude amalgamated
  // timeseries. By default this period runs from twenty four hours before the start of the current task run (either last task run end or current task run start)
  // through to the completion time of the current task run. The number of hours before the start of the current task run can be overridden by
  // the FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS environment variable.
  // startCreationTime and endCreationTime specifiy the period in which to search for any new timeseries created in the core engine.
  let createdStartTime
  if (routeData.previousTaskRunCompletionTime) {
    context.log.info(`The previous task run had the id: '${routeData.previousTaskRunId}'. This task run finished at ${routeData.previousTaskRunCompletionTime}, this will be used as the starting date for the next taskrun search.`)
    createdStartTime = routeData.previousTaskRunCompletionTime
  } else {
    context.log.info(`This is the first task run processed for the non-display group workflow: '${routeData.workflowId}'`)
    createdStartTime = routeData.taskRunStartTime
  }
  // The latest taskrun time is used as the start of the query window
  routeData.createdStartTime = moment(createdStartTime).toISOString()
  routeData.createdEndTime = routeData.taskRunCompletionTime

  // for each filter within the task run
  const timeseriesNonDisplayGroupsData = []
  for (const filter of nonDisplayGroupData) {
    const filterId = `&filterId=${filter.filterId}`

    // get the override values from non-display group reference data from staging for the filter-workflow combination
    let truncationOffsetHoursBackward
    let truncationOffsetHoursForward
    // is approval status required?
    if (filter.approvalRequired && filter.approvalRequired === true) {
      if (routeData.approved === true) {
        context.log.info(`The filter: ${filter.filterId} requires approval and has been approved.`)
      } else {
        context.log.error(`The filter: ${filter.filterId} requires approval and has NOT been approved.`)
        continue // exit the current iteration for this loop as this filter has not been approved
      }
    }

    if (filter.startTimeOffset && filter.startTimeOffset !== 0) {
      truncationOffsetHoursBackward = Math.abs(filter.startTimeOffset)
    } else {
      truncationOffsetHoursBackward = process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS'] ? parseInt(process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS']) : 24
    }
    if (filter.endTimeOffset && filter.endTimeOffset !== 0) {
      truncationOffsetHoursForward = Math.abs(filter.endTimeOffset)
    } else {
      truncationOffsetHoursForward = 0
    }

    // the creation times search within a period for a task that produced/imported timeseries
    const fewsCreatedStartTime = `&startCreationTime=${routeData.createdStartTime.substring(0, 19)}Z`
    const fewsCreatedEndTime = `&endCreationTime=${routeData.createdEndTime.substring(0, 19)}Z`
    // the start time and end time parameters are used to truncate older, amalgamated timeseries created since the previous task run of the workflow
    const fewsStartTime = `&startTime=${moment(routeData.createdStartTime).subtract(truncationOffsetHoursBackward, 'hours').toISOString().substring(0, 19)}Z`
    const fewsEndTime = `&endTime=${moment(routeData.createdEndTime).add(truncationOffsetHoursForward, 'hours').toISOString().substring(0, 19)}Z`

    let fewsParameters
    if (filter.timeseriesType && (filter.timeseriesType === EXTERNAL_HISTORICAL || filter.timeseriesType === EXTERNAL_FORECAST)) {
      fewsParameters = `${filterId}${fewsStartTime}${fewsEndTime}${fewsCreatedStartTime}${fewsCreatedEndTime}`
    } else if (filter.timeseriesType && filter.timeseriesType === SIMULATED_FORECASTING) {
      fewsParameters = `${filterId}${fewsStartTime}${fewsEndTime}`
    } else {
      context.log.error(`There is no recognizable timeseries type specified for the filter: ${filterId}. Filter query cancelled.`)
      // fews parameters must be specified otherwise the data return is likely to be very large
      // exit the current iteration for this filter as there would be no time parameters set
      continue
    }

    // Get the timeseries display groups for the configured plot, locations and date range.
    const fewsPiEndpoint = encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries?useDisplayUnits=false&showThresholds=true&showProducts=false&omitMissing=true&onlyHeaders=false&showEnsembleMemberIds=false&documentVersion=1.26&documentFormat=PI_JSON&forecastCount=1${fewsParameters}`)

    const axiosConfig = {
      method: 'get',
      url: fewsPiEndpoint,
      responseType: 'stream'
    }
    context.log(`Retrieving timeseries display groups for filter ID: ${filterId}`)

    let fewsResponse
    fewsResponse = await axios(axiosConfig)

    timeseriesNonDisplayGroupsData.push({
      fewsParameters: fewsParameters,
      fewsData: await gzip(fewsResponse.data)
    })
  }
  return timeseriesNonDisplayGroupsData
}
