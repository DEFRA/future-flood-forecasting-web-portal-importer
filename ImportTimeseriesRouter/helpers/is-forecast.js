const getBooleanIndicator = require('./get-boolean-indicator')

module.exports = async function isForecast (context, preparedStatement, message) {
  return getBooleanIndicator(context, preparedStatement, message, 'Forecast')
}
