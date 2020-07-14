const getBooleanIndicator = require('./get-boolean-indicator')

module.exports = async function (context, preparedStatement, taskRunData) {
  return getBooleanIndicator(context, preparedStatement, taskRunData, 'Forecast')
}
