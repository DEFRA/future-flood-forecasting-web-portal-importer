const getBooleanIndicator = require('./get-boolean-indicator')

module.exports = async function isTaskRunApproved (context, preparedStatement, routeData) {
  return getBooleanIndicator(context, preparedStatement, routeData, 'Approved')
}
