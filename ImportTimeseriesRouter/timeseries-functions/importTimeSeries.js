const axios = require('axios')
const moment = require('moment')
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

  let createdStartTime
  let startTimeOffset

  const truncationOffsetHours = process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS'] ? parseInt(process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS']) : 24

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
  startTimeOffset = moment(createdStartTime).subtract(truncationOffsetHours, 'hours').toISOString()

  // createdTime specifies the period in which to search for any new task run creations
  const fewsCreatedStartTime = `&startCreationTime=${createdStartTime.substring(0, 19)}Z`
  const fewsCreatedEndTime = `&endCreationTime=${routeData.endTime.substring(0, 19)}Z`

  const fewsStartTime = `&startTime=${startTimeOffset.substring(0, 19)}Z`
  const fewsEndTime = `&endTime=${routeData.endTime.substring(0, 19)}Z`

  const timeseriesNonDisplayGroupsData = []

  for (const value of nonDisplayGroupData) {
    const filterId = `&filterId=${value}`
    const fewsParameters = `${filterId}${fewsStartTime}${fewsEndTime}${fewsCreatedStartTime}${fewsCreatedEndTime}`

    // Get the timeseries display groups for the configured plot, locations and date range.
    const fewsPiEndpoint = encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries?useDisplayUnits=false&showThresholds=true&showProducts=false&omitMissing=true&onlyHeaders=false&showEnsembleMemberIds=false&documentVersion=1.26&documentFormat=PI_JSON&forecastCount=1${fewsParameters}`)
    const fewsResponse = await axios.get(fewsPiEndpoint)

    timeseriesNonDisplayGroupsData.push({
      fewsParameters: fewsParameters,
      fewsData: JSON.stringify(fewsResponse.data)
    })
  }
  return timeseriesNonDisplayGroupsData
}
