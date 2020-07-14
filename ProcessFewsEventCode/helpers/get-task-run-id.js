const extract = require('./extract')

module.exports = async function (context, preparedStatement, taskRunData) {
  const taskRunIdRegex = /\sid(?:\s*=?\s*)([^\s)]*)(?:\s*\)?)/i
  const taskRunIdText = 'task run ID'
  return extract(context, taskRunData, taskRunIdRegex, 2, 1, taskRunIdText, preparedStatement)
}
