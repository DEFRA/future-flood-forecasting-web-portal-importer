const extract = require('./extraction-utils')

// INC2158095 - Allow correct matching when a workflow ID begins with the characters id (case insensitive).
const taskRunIdRegex = /\s(?:(?:with\s+id\s*)|(?:id\s*=\s*))([^\s)]*)(?:\s*\)?)/i
const taskRunIdText = 'task run ID'

module.exports = async function (context, taskRunData) {
  return await extract(context, taskRunData, taskRunIdRegex, taskRunIdText)
}
