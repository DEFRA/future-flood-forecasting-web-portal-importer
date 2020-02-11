const createStagingException = require('../../Shared/create-staging-exception')

module.exports = async function preprocessMessage (context, preparedStatement, message) {
  const errorMessage = 'Message must be either a string or a pure object'
  let returnValue = null

  if (message) {
    switch (message.constructor.name) {
      case 'String':
        returnValue = message
        break
      case 'Object':
        returnValue = JSON.stringify(message)
        break
      default:
        await createStagingException(context, preparedStatement, message, errorMessage)
        break
    }
  } else {
    await createStagingException(context, preparedStatement, message, errorMessage)
  }
  return returnValue
}
