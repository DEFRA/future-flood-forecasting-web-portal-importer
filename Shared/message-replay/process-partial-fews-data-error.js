const { getEnvironmentVariableAsAbsoluteInteger } = require('../../Shared/utils')
const moment = require('moment')

// Delay message replay for thirty seconds by default to mitigate the risk of PI Server overload.
const MESSAGE_REPLAY_DELAY_MILLIS =
  getEnvironmentVariableAsAbsoluteInteger('CHECK_FOR_TASK_RUN_MISSING_EVENTS_DELAY_MILLIS') || 30000

module.exports = function (context, message, bindingName) {
  // Schedule the message being processed for replay on a NON-EXISTENT output binding for the same
  // queue from which the message being processed was received.
  // Use of a non-existent output binding allows common code to be used when ensuring scheduled messages placed on output bindings
  // are published manually to workaround https://github.com/Azure/Azure-Functions/issues/454.
  const scheduledEnqueueTimeUtc = moment.utc().add(MESSAGE_REPLAY_DELAY_MILLIS, 'milliseconds').toDate()
  context.bindings[bindingName] = [{
    body: message,
    scheduledEnqueueTimeUtc
  }]
}
