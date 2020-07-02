const axios = require('axios')
const moment = require('moment')
const { gzip } = require('../../Shared/utils')
const getOverrideValues = require('../helpers/get-ndg-override-values')
const getLatestEndTime = require('../helpers/get-latest-task-run-end-time')
const { executePreparedStatementInTransaction } = require('../../Shared/transaction-helper')

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
    nonDisplayGroupData.push(record.filter_id)
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

  // for each filter within the taskrun
  const timeseriesNonDisplayGroupsData = []
  for (const filterValue of nonDisplayGroupData) {
    const filterId = `&filterId=${filterValue}`

    // get the override values from non-display group reference data from staging for the filter-workflow combination
    let truncationOffsetHoursBackward
    let truncationOffsetHoursForward
    await executePreparedStatementInTransaction(getOverrideValues, context, routeData.transaction, routeData, filterValue)
    // is approval status required?
    if (routeData.approvalRequired === true) {
      if (routeData.approved === true) {
        context.log.info(`The filter: ${filterId} requires approval and has been approved.`)
      } else {
        context.log.error(`The filter: ${filterId} requires approval and has NOT been approved.`)
        break // exit the loop for this filter has not been approved
      }
    }
    if (routeData.startTimeOverrideRequired === true) {
      truncationOffsetHoursBackward = routeData.ndgOversetOverrideBackward
    } else {
      truncationOffsetHoursBackward = process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS'] ? parseInt(process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS']) : 24
    }
    if (routeData.endTimeOverrideRequired === true) {
      truncationOffsetHoursForward = routeData.ndgOversetOverrideForward
    } else {
      truncationOffsetHoursForward = 0
    }

    // the start time and end time parameters are used to exclude older,
    // amalgamated timeseries created since the previous task run of the workflow.
    const startTimeOffset = moment(routeData.createdStartTime).subtract(truncationOffsetHoursBackward, 'hours').toISOString()
    const endTimeOffset = moment(routeData.createdEndTime).add(truncationOffsetHoursForward, 'hours').toISOString()

    // creation time search period for a task that produced/imported timeseries
    const fewsCreatedStartTime = `&startCreationTime=${routeData.createdStartTime.substring(0, 19)}Z`
    const fewsCreatedEndTime = `&endCreationTime=${routeData.createdEndTime.substring(0, 19)}Z`
    // data truncation period
    const fewsStartTime = `&startTime=${startTimeOffset.substring(0, 19)}Z`
    const fewsEndTime = `&endTime=${endTimeOffset.substring(0, 19)}Z`

    let fewsParameters
    if (routeData.timeseriesType === 'external_historical' || routeData.timeseriesType === 'external_forecasting') {
      fewsParameters = `${filterId}${fewsStartTime}${fewsEndTime}${fewsCreatedStartTime}${fewsCreatedEndTime}`
    } else if (routeData.timeseriesType === 'simulated_forecasting') {
      fewsParameters = `${filterId}${fewsStartTime}${fewsEndTime}`
    } else {
      context.log.error(`Timeseries type for the filter: ${filterId} requires approval and has NOT been approved.`)
      break // exit the loop for this filter as there are no search parameters the filter-workflow combination is abandoned
    }

    // Get the timeseries display groups for the configured plot, locations and date range.
    const fewsPiEndpoint = encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries?useDisplayUnits=false&showThresholds=true&showProducts=false&omitMissing=true&onlyHeaders=false&showEnsembleMemberIds=false&documentVersion=1.26&documentFormat=PI_JSON&forecastCount=1${fewsParameters}`)

    const axiosConfig = {
      method: 'get',
      url: fewsPiEndpoint,
      responseType: 'stream'
    }
    context.log(`Retrieving timeseries display groups for filter ID ${filterId}`)

    let fewsResponse
    // fews parameters must be speified otherwise the data returnd would be too large
    if (fewsParameters && fewsParameters.length > 0) {
      fewsResponse = await axios(axiosConfig)
    }

    timeseriesNonDisplayGroupsData.push({
      fewsParameters: fewsParameters,
      fewsData: await gzip(fewsResponse.data)
    })
  }
  return timeseriesNonDisplayGroupsData
}
