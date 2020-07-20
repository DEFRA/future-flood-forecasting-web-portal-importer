// Adapted from https://medium.com/@xjamundx/custom-javascript-errors-in-es6-aa891b173f87
module.exports = class TimeseriesStagingError extends Error {
  constructor (context, ...args) {
    super(...args)
    Error.captureStackTrace(this, TimeseriesStagingError)
    this.context = context
  }
}
