const extract = require('./extraction-utils')

const taskRunIdRegex = /\sid(?:\s*=?\s*)([^\s)]*)(?:\s*\)?)/i
const taskRunIdText = 'task run ID'

module.exports = async function (context, taskRunData) {
  return await extract(context, taskRunData, taskRunIdRegex, taskRunIdText)
}
