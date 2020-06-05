const getBooleanIndicator = require('./get-boolean-indicator')

module.exports = async function isForecast (context, preparedStatement, routeData) {
  return getBooleanIndicator(context, preparedStatement, routeData, 'Forecast')
}
