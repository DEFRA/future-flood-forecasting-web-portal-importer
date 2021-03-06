const extract = require('./extraction-utils')

module.exports = async function (context, taskRunData, indicatorName) {
  let indicator = taskRunData.message.includes('is made current manually')

  if (!indicator) {
    const indicatorRegex = new RegExp(`(?:${indicatorName}\\:?\\s*(True|False))`, 'i')
    const indicatorText = `task run ${indicatorName} status`

    const indicatorString = await extract(context, taskRunData, indicatorRegex, indicatorText)

    if (typeof indicatorString === 'undefined') {
      indicator = undefined
    } else {
      indicator = indicatorString && !!indicatorString.match(/true/i)
    }
  }
  return Promise.resolve(indicator)
}
