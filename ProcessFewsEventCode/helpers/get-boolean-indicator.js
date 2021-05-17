const extract = require('./extract')

const expectedNumberOfMatches = 2
const matchIndexToReturn = 1

module.exports = async function (context, taskRunData, indicatorName) {
  let indicator = taskRunData.message.includes('is made current manually')

  if (!indicator) {
    const indicatorRegex = new RegExp(`(?:${indicatorName}\\:?\\s*(True|False))`, 'i')
    const indicatorText = `task run ${indicatorName} status`

    const extractionData = {
      taskRunData,
      regex: indicatorRegex,
      expectedNumberOfMatches,
      matchIndexToReturn,
      errorMessageSubject: indicatorText
    }

    const indicatorString = await extract(context, extractionData)

    if (typeof indicatorString === 'undefined') {
      indicator = undefined
    } else {
      indicator = indicatorString && !!indicatorString.match(/true/i)
    }
  }
  return Promise.resolve(indicator)
}
