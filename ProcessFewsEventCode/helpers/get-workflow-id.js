const extract = require('./extraction-utils')

const workflowIdRegex = /task(?:\s+run)?\s+([^\s]*)\s+/i
const workflowIdText = 'workflow ID'

module.exports = async function (context, taskRunData) {
  return await extract(context, taskRunData, workflowIdRegex, workflowIdText)
}
