import extract from './extraction-utils.js'

// INC2158095 - Allow correct matching when a workflow ID begins with the characters id (case insensitive).
const taskRunIdRegex = /\s(?:(?:with\s+id\s*)|(?:id\s*=\s*))([^\s)]*)(?:\s*\)?)/i
const taskRunIdText = 'task run ID'

export default async function (context, taskRunData) {
  return await extract(context, taskRunData, taskRunIdRegex, taskRunIdText)
}
