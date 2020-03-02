const axios = require('axios')

module.exports = async function getTimeseries (context, routeData) {
  const nonDisplayGroupData = await getNonDisplayGroupData(routeData.nonDisplayGroupWorkflowsResponse)
  const timeseries = await getTimeseriesInternal(nonDisplayGroupData, routeData)
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

async function getTimeseriesInternal (nonDisplayGroupData, routeData) {
  // The database in which data is loaded requires fractional seconds to be included in dates. By contrast
  // the REST interface of the core forecasting engine requires fractional seconds to be excluded from dates.
  const fewsStartTime = `&startTime=${routeData.startTime.substring(0, 19)}Z`
  const fewsEndTime = `&endTime=${routeData.endTime.substring(0, 19)}Z`
  const fewsTaskRunIds = `&taskRunIds=${routeData.taskId}`

  const timeseriesNonDisplayGroupsData = []

  for (const value of nonDisplayGroupData) {
    const filterId = `&filterId=${value}`
    const fewsParameters = `${filterId}${fewsTaskRunIds}${fewsStartTime}${fewsEndTime}`

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
