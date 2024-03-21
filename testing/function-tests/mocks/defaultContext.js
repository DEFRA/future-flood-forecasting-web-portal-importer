const { logger } = require('defra-logging-facade')

module.exports = function () {
  this.bindingData = {
    deliveryCount: 0
  }
  this.bindings = {}
  this.bindingDefinitions = []
  this.done = jest.fn(logger.info('context.done() called'))
  this.log = (function () {
    const defaultLogFunction = jest.fn(message => logger.info(message))
    defaultLogFunction.info = jest.fn(message => logger.info(message))
    defaultLogFunction.warn = jest.fn((message) => logger.warn(message))
    defaultLogFunction.error = jest.fn(message => logger.error(message))
    return defaultLogFunction
  })()
  return this
}
