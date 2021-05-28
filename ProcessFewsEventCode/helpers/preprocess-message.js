const createStagingException = require('../../Shared/timeseries-functions/create-staging-exception')

const errorMessage = 'Message must be either a string or a pure object'

module.exports = async function (context, transaction, message) {
  let returnValue = null

  if (message) {
    returnValue = await preprocessMessageInternal(context, transaction, message)
  } else {
    returnValue = await preprocessErroneousMessage(context, transaction, message)
  }
  return returnValue
}

async function preprocessMessageInternal (context, transaction, message) {
  let returnValue = null
  switch (message.constructor.name) {
    case 'String':
      returnValue = Promise.resolve(message)
      break
    case 'Object':
      returnValue = Promise.resolve(JSON.stringify(message))
      break
    default:
      returnValue = createStagingException(context, { message, transaction, errorMessage, sourceFunction: 'P' })
      break
  }
  return returnValue
}

async function preprocessErroneousMessage (context, transaction, message) {
  let returnValue = null
  if (typeof message === 'undefined') {
    context.log.warn('Ignoring undefined message')
  } else if (typeof message === 'string' && message.length === 0) {
    context.log.warn('Ignoring message with empty content')
  } else {
    returnValue = createStagingException(context, { message: message, transaction: transaction, errorMessage: errorMessage, sourceFunction: 'P' })
  }
  return returnValue
}
