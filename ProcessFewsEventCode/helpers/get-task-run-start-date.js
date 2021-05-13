import extract from './extraction-utils.js'

// ISO-8601 date time regular expression adapted from Regular Expressions Cookbook, 2nd Edition by Steven Levithan, Jan Goyvaerts
const taskRunStartDateRegex = /(?:start time(?::)?|T0\s*=?)\s*((?:[0-9]{4})-?(?:1[0-2]|0[1-9])-?(?:3[01]|0[1-9]|[12][0-9]) (?:2[0-3]|[01][0-9]):?(?:[0-5][0-9]):?(?:[0-5][0-9])?)/i
const taskRunStartDateText = 'task run start date'

export default async function (context, taskRunData) {
  return await extract(context, taskRunData, taskRunStartDateRegex, taskRunStartDateText)
}
