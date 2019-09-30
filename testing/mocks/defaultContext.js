const { logger } = require('defra-logging-facade')

module.exports = function () {
  this.log = (function () {
    const defaultLogFunction = jest.fn(message => logger.info(message))
    defaultLogFunction.info = jest.fn(message => logger.info(message))
    defaultLogFunction.warn = jest.fn((message) => logger.warn(message))
    defaultLogFunction.error = jest.fn(message => logger.error(message))
    return defaultLogFunction
  })()
  return this
}
