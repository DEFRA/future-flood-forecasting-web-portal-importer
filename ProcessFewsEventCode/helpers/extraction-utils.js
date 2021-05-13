import extract from './extract.js'

const expectedNumberOfMatches = 2
const matchIndexToReturn = 1

export default async function (context, taskRunData, regex, errorMessageSubject) {
  const extractionData = {
    taskRunData,
    regex,
    expectedNumberOfMatches,
    matchIndexToReturn,
    errorMessageSubject
  }

  return await extract(context, extractionData)
}
