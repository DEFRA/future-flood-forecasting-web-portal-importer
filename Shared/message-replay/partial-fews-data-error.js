// Adapted from https://medium.com/@xjamundx/custom-javascript-errors-in-es6-aa891b173f87
export default class PartialFewsDataError extends Error {
  constructor (config, ...args) {
    super(...args)
    Error.captureStackTrace(this, PartialFewsDataError)
    this.context = config.context
    this.messageToReplay = config.messageToReplay
    this.replayDelayMillis = config.replayDelayMillis
    this.bindingName = config.bindingName
  }
}
