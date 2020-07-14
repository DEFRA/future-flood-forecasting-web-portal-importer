const extract = require('./extract')

module.exports = async function (context, preparedStatement, taskRunData) {
  const workflowIdRegex = /task(?:\s+run)?\s+([^\s]*)\s+/i
  const workflowIdText = 'workflow ID'
  return extract(context, taskRunData, workflowIdRegex, 2, 1, workflowIdText, preparedStatement)
}
