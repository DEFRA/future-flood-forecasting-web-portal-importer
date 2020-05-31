const extract = require('../../Shared/extract')

module.exports = async function getTaskRunId (context, preparedStatement, routeData) {
  const taskRunIdRegex = /\sid(?:\s*=?\s*)([^\s)]*)(?:\s*\)?)/i
  const taskRunIdText = 'task run ID'
  return extract(context, routeData, taskRunIdRegex, 2, 1, taskRunIdText, preparedStatement)
}
