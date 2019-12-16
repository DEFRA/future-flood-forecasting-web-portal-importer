const extract = require('../../Shared/extract')

module.exports = async function getTaskRunCompletionDate (context, message, preparedStatement) {
  const taskRunCompletionDateRegex = / end time(?::)? ([^ ]*) /i
  const taskRunCompletionDateText = 'task run completion date'
  return extract(context, message, taskRunCompletionDateRegex, 2, 1, taskRunCompletionDateText, preparedStatement)
}
