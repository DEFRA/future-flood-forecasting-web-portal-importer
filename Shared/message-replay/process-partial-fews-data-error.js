const moment = require('moment')

module.exports = function (err) {
  const context = err.context
  context.log.warn(err.message)
  // Schedule the message being processed for replay on a NON-EXISTENT output binding for the same
  // queue from which the message being processed was received.
  // Use of a non-existent output binding allows common code to be used when ensuring scheduled messages placed on output bindings
  // are published manually to workaround https://github.com/Azure/Azure-Functions/issues/454.
  const scheduledEnqueueTimeUtc = moment.utc().add(err.replayDelayMillis, 'milliseconds').toDate()
  context.bindings[err.bindingName] = [{
    body: err.messageToReplay,
    scheduledEnqueueTimeUtc
  }]
}
