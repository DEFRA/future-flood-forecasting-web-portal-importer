import extract from './extraction-utils.js'

const taskRunIdRegex = /\sid(?:\s*=?\s*)([^\s)]*)(?:\s*\)?)/i
const taskRunIdText = 'task run ID'

export default async function (context, taskRunData) {
  return await extract(context, taskRunData, taskRunIdRegex, taskRunIdText)
}
