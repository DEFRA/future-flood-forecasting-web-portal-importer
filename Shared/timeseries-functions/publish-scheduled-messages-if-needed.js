import { publishMessages } from '../service-bus-helper.js'

export default async function (context, config) {
  const scheduledMessages = context.bindings[config.outputBinding]?.filter(message => message.scheduledEnqueueTimeUtc)

  if (scheduledMessages?.length > 0) {
    const taskRunId = config.outputBinding === 'importFromFews'
      ? scheduledMessages[0].body.taskRunId
      : context.taskRunId

    // https://github.com/Azure/Azure-Functions/issues/454
    // Scheduled messages cannot be published to Azure Service Bus using an Azure Function
    // output binding. Scheduled messages need to be published manually.
    context.log(`Publishing ${scheduledMessages.length} scheduled message(s) manually for task run ${taskRunId}`)
    await publishMessages({
      context,
      destinationName: config.destinationName,
      fn: () => context.bindings[config.outputBinding]
    })

    // Prevent outgoing messages from being published using the output binding.
    context.bindings[config.outputBinding] = []
  }
}
