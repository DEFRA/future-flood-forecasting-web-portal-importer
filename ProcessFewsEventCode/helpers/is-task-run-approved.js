const getBooleanIndicator = require('./get-boolean-indicator')

module.exports = async function (context, taskRunData) {
  return getBooleanIndicator(context, taskRunData, 'Approved')
}
