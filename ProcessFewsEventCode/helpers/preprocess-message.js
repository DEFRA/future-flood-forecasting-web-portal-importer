const createOrReplaceStagingException = require('../../Shared/timeseries-functions/create-or-replace-staging-exception')

module.exports = async function (context, transaction, message) {
  const errorMessage = 'Message must be either a string or a pure object'
  let returnValue = null

  if (message) {
    switch (message.constructor.name) {
      case 'String':
        returnValue = Promise.resolve(message)
        break
      case 'Object':
        returnValue = Promise.resolve(JSON.stringify(message))
        break
      default:
        returnValue = createOrReplaceStagingException(context, { message: message, transaction: transaction, errorMessage: errorMessage, sourceFunction: 'P' })
        break
    }
  } else {
    if (typeof message === 'undefined') {
      context.log.warn('Ignoring undefined message')
    } else if (typeof message === 'string' && message.length === 0) {
      context.log.warn('Ignoring message with empty content')
    } else {
      returnValue = createOrReplaceStagingException(context, { message: message, transaction: transaction, errorMessage: errorMessage, sourceFunction: 'P' })
    }
  }
  return returnValue
}
