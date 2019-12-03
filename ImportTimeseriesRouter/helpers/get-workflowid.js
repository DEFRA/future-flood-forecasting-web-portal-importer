const extract = require('../../Shared/extract')

module.exports = async function getWorkflowId (context, message, preparedStatement) {
  const workflowIdRegex = /task(?: run)? ([^ ]*) /i
  const workflowIdText = 'workflow ID'
  return extract(context, message, workflowIdRegex, 2, 1, workflowIdText, preparedStatement)
}
