const { getEnvironmentVariableAsAbsoluteInteger } = require('../../Shared/utils')
const moment = require('moment')

const MESSAGE_REPLAY_DELAY_MILLIS =
  getEnvironmentVariableAsAbsoluteInteger('CHECK_FOR_TASK_RUN_DATA_AVAILABILITY_DELAY_MILLIS') || 2000

module.exports = function (context, message, bindingName) {
  // Schedule the message being processed for replay on a NON-EXISTENT output binding.
  // Use of a non-existent output binding allows common code to be used when ensuring scheduled messages placed on output bindings
  // are published manually to workaround https://github.com/Azure/Azure-Functions/issues/454.
  const scheduledEnqueueTimeUtc = moment.utc().add(MESSAGE_REPLAY_DELAY_MILLIS, 'milliseconds').toDate()
  context.bindings[bindingName] = [{
    body: message,
    scheduledEnqueueTimeUtc
  }]
}
