const extract = require('./extract')

module.exports = async function (context, taskRunData) {
  const expectedNumberOfMatches = 2
  const matchIndexToReturn = 1
  const taskRunIdRegex = /\sid(?:\s*=?\s*)([^\s)]*)(?:\s*\)?)/i
  const taskRunIdText = 'task run ID'

  const extractionData = {
    taskRunData,
    regex: taskRunIdRegex,
    expectedNumberOfMatches,
    matchIndexToReturn,
    errorMessageSubject: taskRunIdText
  }

  return extract(context, extractionData)
}
