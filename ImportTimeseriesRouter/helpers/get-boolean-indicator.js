const extract = require('../../Shared/extract')

module.exports = async function getIndicator (context, preparedStatement, message, indicatorName) {
  let indicator = message.includes('is made current manually')

  if (!indicator) {
    const indicatorRegex = new RegExp(`(?:${indicatorName}\\:?\\s*(True|False))`, 'i')
    const indicatorText = 'task run approval status'
    const indicatorString = await extract(context, message, indicatorRegex, 2, 1, indicatorText, preparedStatement)

    if (typeof indicatorString === 'undefined') {
      indicator = undefined
    } else {
      indicator = indicatorString && !!indicatorString.match(/true/i)
    }
  }
  return indicator
}
