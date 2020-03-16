// Adapted from https://medium.com/@xjamundx/custom-javascript-errors-in-es6-aa891b173f87
module.exports = class StagingError extends Error {
  constructor (...args) {
    super(...args)
    Error.captureStackTrace(this, StagingError)
  }
}
