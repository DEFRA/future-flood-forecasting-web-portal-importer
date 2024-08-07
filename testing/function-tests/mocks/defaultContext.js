import { logger } from '../../../Shared/utils'
import { vi } from 'vitest'

export default function () {
  this.bindingData = {
    deliveryCount: 0
  }
  this.bindings = {}
  this.bindingDefinitions = []
  this.done = vi.fn(logger.info('context.done() called'))
  this.log = (function () {
    const defaultLogFunction = vi.fn(message => logger.info(message))
    defaultLogFunction.info = vi.fn(message => logger.info(message))
    defaultLogFunction.warn = vi.fn((message) => logger.warn(message))
    defaultLogFunction.error = vi.fn(message => logger.error(message))
    return defaultLogFunction
  })()
  return this
}
