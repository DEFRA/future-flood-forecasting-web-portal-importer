const extract = require('./extract')

const expectedNumberOfMatches = 2
const matchIndexToReturn = 1

module.exports = async function (context, taskRunData, regex, errorMessageSubject) {
  const extractionData = {
    taskRunData,
    regex,
    expectedNumberOfMatches,
    matchIndexToReturn,
    errorMessageSubject
  }

  return await extract(context, extractionData)
}
