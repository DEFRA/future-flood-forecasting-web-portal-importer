import extract from './extraction-utils.js'

const workflowIdRegex = /task(?:\s+run)?\s+([^\s]*)\s+/i
const workflowIdText = 'workflow ID'

export default async function (context, taskRunData) {
  return await extract(context, taskRunData, workflowIdRegex, workflowIdText)
}
