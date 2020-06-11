const axios = require('axios')
const { gzip } = require('../../Shared/utils')

module.exports = async function getTimeseriesDisplayGroups (context, routeData) {
  const displayGroupData = await getDisplayGroupData(routeData.displayGroupWorkflowsResponse)
  const timeseriesDisplayGroups = await getTimeseriesDisplayGroupsInternal(context, displayGroupData, routeData)
  return timeseriesDisplayGroups
}

async function getDisplayGroupData (displayGroupWorkflowsResponse) {
  // Get the plot identifiers needed to retrieve timeseries display groups
  // from the REST interface of the core forecasting engine.
  const displayGroupData = {}

  for (const record of displayGroupWorkflowsResponse.recordset) {
    displayGroupData[record.plot_id] = record.location_ids
  }

  return displayGroupData
}

async function getTimeseriesDisplayGroupsInternal (context, displayGroupData, routeData) {
  // The database in which data is loaded requires fractional seconds to be included in dates. By contrast
  // the REST interface of the core forecasting engine requires fractional seconds to be excluded from dates.
  const fewsStartTime = `&startTime=${routeData.startTimeDG.substring(0, 19)}Z`
  const fewsEndTime = `&endTime=${routeData.endTimeDG.substring(0, 19)}Z`

  const timeseriesDisplayGroupsData = []

  for (const key of Object.keys(displayGroupData)) {
    const plotId = `&plotId=${key}`
    const locationIds = `&locationIds=${displayGroupData[key].replace(/;/g, '&locationIds=')}`
    const fewsParameters = `${plotId}${locationIds}${fewsStartTime}${fewsEndTime}`

    // Get the timeseries display groups for the configured plot, locations and date range.
    const fewsPiEndpoint =
     encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries/displaygroups?useDisplayUnits=false
       &showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON${fewsParameters}`)

    const axiosConfig = {
      method: 'get',
      url: fewsPiEndpoint,
      responseType: 'stream'
    }
    context.log(`Retrieving timeseries display groups for plot ID ${plotId}`)

    const fewsResponse = await axios(axiosConfig)

    context.log(`Preparing retrieved timeseries display groups for plot ID ${plotId}`)

    timeseriesDisplayGroupsData.push({
      fewsParameters: fewsParameters,
      fewsData: await gzip(fewsResponse.data)
    })
  }
  return timeseriesDisplayGroupsData
}
