const extract = require('../../Shared/extract')

module.exports = async function getTaskRunId (context, preparedStatement, message) {
  const taskRunIdRegex = / id(?: |=)([^ )]*)(?: |\))/i
  const taskRunIdText = 'task run ID'
  return extract(context, message, taskRunIdRegex, 2, 1, taskRunIdText, preparedStatement)
}
