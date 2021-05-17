const extract = require('./extract')

const expectedNumberOfMatches = 2
const matchIndexToReturn = 1
// ISO-8601 date time regular expression adapted from Regular Expressions Cookbook, 2nd Edition by Steven Levithan, Jan Goyvaerts
const taskRunCompletionDateRegex = / (?:end time(?::)?|dispatch\s*=?)\s*((?:[0-9]{4})-?(?:1[0-2]|0[1-9])-?(?:3[01]|0[1-9]|[12][0-9]) (?:2[0-3]|[01][0-9]):?(?:[0-5][0-9]):?(?:[0-5][0-9])?)/i
const taskRunCompletionDateText = 'task run completion date'

module.exports = async function (context, taskRunData) {
  const extractionData = {
    taskRunData,
    regex: taskRunCompletionDateRegex,
    expectedNumberOfMatches,
    matchIndexToReturn,
    errorMessageSubject: taskRunCompletionDateText
  }

  return extract(context, extractionData)
}
