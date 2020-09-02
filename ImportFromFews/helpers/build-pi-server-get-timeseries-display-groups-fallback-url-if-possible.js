const getUnknownLocationsFromPiServerErrorMessage = require('../../Shared/timeseries-functions/get-unknown-locations-from-pi-server-error-message')

module.exports = async function (context, taskRunData) {
  context.log(`Attempting to filter out unknown locations for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  await buildKnownLocations(context, taskRunData)
  if (taskRunData.knownLocationsIds.length > 0) {
    context.log(`Found known locations (${taskRunData.knownLocationsIds}) (for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
    const buildPiServerUrlCall = taskRunData.buildPiServerUrlCalls[taskRunData.piServerUrlCallsIndex]
    const plotId = `&plotId=${taskRunData.plotId}`
    const locationIds = `&locationIds=${taskRunData.knownLocationsIds.replace(/;/g, '&locationIds=')}`
    buildPiServerUrlCall.fewsParameters = `${plotId}${locationIds}${taskRunData.fewsStartTime}${taskRunData.fewsEndTime}`
    // Construct the URL used to retrieve timeseries display groups for the configured plot, locations and date range.
    buildPiServerUrlCall.fewsPiUrl =
      encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries/displaygroups?useDisplayUnits=false
        &showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON${taskRunData.fewsParameters}`)
  } else {
    context.log.warn(`Unable to find any known locations for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId})`)
  }
}

async function buildKnownLocations (context, taskRunData) {
  const errorMessageFromOriginalPiServerCall = taskRunData.buildPiServerUrlCalls[0].error.response.data
  const unknownLocationsIdsFromPiServerError = await getUnknownLocationsFromPiServerErrorMessage(context, errorMessageFromOriginalPiServerCall)
  const allLocationsIds = new Set(taskRunData.locationIds.split(';'))
  // Calculate the difference between the set of all locations eligible for import and the set of unknown locations.
  const knownLocationsIds = new Set([...allLocationsIds].filter(locationId => !unknownLocationsIdsFromPiServerError.has(locationId)))
  taskRunData.knownLocationsIds = [...knownLocationsIds].join(';')
}
