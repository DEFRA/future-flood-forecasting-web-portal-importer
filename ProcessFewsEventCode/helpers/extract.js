const createOrReplaceStagingException = require('../../Shared/timeseries-functions/create-or-replace-staging-exception')

module.exports = async function (context, taskRunData, regex, expectedNumberOfMatches, matchIndexToReturn, errorMessageSubject) {
  const matches = regex.exec(taskRunData.message)
  // If the message contains the expected number of matches from the specified regular expression return
  // the match indicated by the caller.
  if (matches && matches.length === expectedNumberOfMatches) {
    return Promise.resolve(matches[matchIndexToReturn])
  } else {
    // If regular expression matching did not complete successfully, the message is not in an expected
    // format and cannot be replayed. In this case intervention is needed so create a staging
    // exception.
    taskRunData.errorMessage = `Unable to extract ${errorMessageSubject} from message`
    return createOrReplaceStagingException(context, taskRunData)
  }
}
