const axios = require('axios')
const moment = require('moment')
const { gzip } = require('../../Shared/utils')
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
  // timeseries. By default this period runs from twenty four hours before the start of the current task run through to the
  // completion time of the current task run. The number of hours before the start of the current task run can be overridden by
  // the FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS environment variable.
  let createdStartTime

  if (routeData.previousTaskRunCompletionTime) {
    context.log.info(`The previous task run had the id: '${routeData.previousTaskRunId}'. This task run finished at ${routeData.previousTaskRunCompletionTime}, this will be used as the starting date for the next taskrun search.`)
    createdStartTime = routeData.previousTaskRunCompletionTime
  } else {
    context.log.info(`This is the first task run processed for the non-display group workflow: '${routeData.workflowId}'`)
    createdStartTime = routeData.taskRunStartTime
  }

  // Overwrite the startTime and endTime values that were intially set for forecast workflows
  // to equal the 'createdStartTime/createdEndTime' set for observed data.
  routeData.startTime = moment(createdStartTime).toISOString()
  routeData.endTime = routeData.taskRunCompletionTime

  // startCreationTime and endCreationTime specifiy the period in which to search for any new timeseries created
  // in the core engine.
  const fewsCreatedStartTime = `&startCreationTime=${createdStartTime.substring(0, 19)}Z`
  const fewsCreatedEndTime = `&endCreationTime=${routeData.endTime.substring(0, 19)}Z`

  // startTime and endTime specify the period to which timeseries are associated. This period is used to exclude older,
  // amalgamated timeseries created since the previous task run of the workflow.
  const truncationOffsetHours = process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS'] ? parseInt(process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS']) : 24
  const startTimeOffset = moment(createdStartTime).subtract(truncationOffsetHours, 'hours').toISOString()
  const fewsStartTime = `&startTime=${startTimeOffset.substring(0, 19)}Z`
  const fewsEndTime = `&endTime=${routeData.endTime.substring(0, 19)}Z`

  const timeseriesNonDisplayGroupsData = []

  for (const value of nonDisplayGroupData) {
    const filterId = `&filterId=${value}`
    const fewsParameters = `${filterId}${fewsStartTime}${fewsEndTime}${fewsCreatedStartTime}${fewsCreatedEndTime}`

    // Get the timeseries display groups for the configured plot, locations and date range.
    const fewsPiEndpoint = encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries?useDisplayUnits=false&showThresholds=true&showProducts=false&omitMissing=true&onlyHeaders=false&showEnsembleMemberIds=false&documentVersion=1.26&documentFormat=PI_JSON&forecastCount=1${fewsParameters}`)

    context.log(`Retrieving timeseries display groups for filter ID ${filterId}`)

    const axiosConfig = {
      method: 'get',
      url: fewsPiEndpoint,
      responseType: 'stream'
    }
    context.log(`Retrieving timeseries display groups for filter ID ${filterId}`)

    const fewsResponse = await axios(axiosConfig)

    timeseriesNonDisplayGroupsData.push({
      fewsParameters: fewsParameters,
      fewsData: await gzip(fewsResponse.data)
    })
  }
  return timeseriesNonDisplayGroupsData
}
