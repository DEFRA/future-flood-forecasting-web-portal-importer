// Adapted from https://medium.com/@xjamundx/custom-javascript-errors-in-es6-aa891b173f87
module.exports = class PartialFewsDataError extends Error {
  constructor (context, incomingMessage, ...args) {
    super(...args)
    Error.captureStackTrace(this, PartialFewsDataError)
    this.context = context
    this.incomingMessage = incomingMessage
  }
}
