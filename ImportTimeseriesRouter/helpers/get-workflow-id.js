const extract = require('../../Shared/extract')

module.exports = async function getWorkflowId (context, preparedStatement, message) {
  const workflowIdRegex = /task(?:\s+run)?\s+([^\s]*)\s+/i
  const workflowIdText = 'workflow ID'
  return extract(context, message, workflowIdRegex, 2, 1, workflowIdText, preparedStatement)
}
