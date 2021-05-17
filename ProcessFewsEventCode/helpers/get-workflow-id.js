const extract = require('./extract')

const expectedNumberOfMatches = 2
const matchIndexToReturn = 1
const workflowIdRegex = /task(?:\s+run)?\s+([^\s]*)\s+/i
const workflowIdText = 'workflow ID'

module.exports = async function (context, taskRunData) {
  const extractionData = {
    taskRunData,
    regex: workflowIdRegex,
    expectedNumberOfMatches,
    matchIndexToReturn,
    errorMessageSubject: workflowIdText
  }
  return extract(context, extractionData)
}
