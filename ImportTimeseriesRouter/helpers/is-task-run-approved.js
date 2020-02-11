const getBooleanIndicator = require('./get-boolean-indicator')

module.exports = async function isTaskRunApproved (context, preparedStatement, message) {
  return getBooleanIndicator(context, preparedStatement, message, 'Approved')
}
