module.exports = async function (context, message) {
  // If JSON parsing fails when a message is sent to the fews-eventcode queue the message
  // will remain on the dead letter queue. It seems that some core forecasting engine messages
  // can contain invalid JSON, causing this scenario. To workaround this temporarily, try and parse
  // the message as JSON before replay is attempted. If parsing fails, stringify the message before replaying it.
  let messageToReplay = message

  if (message.constructor.name === 'String') {
    try {
      JSON.parse(message)
    } catch (err) {
      messageToReplay = JSON.stringify(message)
    }
  }

  if (message.constructor.name === 'Object') {
    context.log(`Replaying ${JSON.stringify(messageToReplay)}`)
  } else {
    context.log(`Replaying ${messageToReplay}`)
  }

  context.bindings.processFewsEventCode = messageToReplay
}
