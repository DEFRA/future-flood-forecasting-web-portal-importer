import createStagingException from '../../Shared/timeseries-functions/create-staging-exception.js'

export default async function (context, extractionData) {
  const matches = extractionData.regex.exec(extractionData.taskRunData.message)
  // If the message contains the expected number of matches from the specified regular expression return
  // the match indicated by the caller.
  if (matches && matches.length === extractionData.expectedNumberOfMatches) {
    return Promise.resolve(matches[extractionData.matchIndexToReturn])
  } else {
    // If regular expression matching did not complete successfully, the message is not in an expected
    // format and cannot be replayed. In this case intervention is needed so create a staging
    // exception.
    extractionData.taskRunData.errorMessage = `Unable to extract ${extractionData.errorMessageSubject} from message`
    return createStagingException(context, extractionData.taskRunData)
  }
}
