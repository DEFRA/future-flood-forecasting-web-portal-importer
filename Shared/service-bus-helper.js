// Code adapted from https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-nodejs-how-to-use-queues-new-package
const azureServiceBus = require('@azure/service-bus')
const { sleep } = require('./utils')
const connectionString = process.env.AzureWebJobsServiceBus

const pauseBeforePropagatingErrorConfig = {
  environmentVariableName: 'PAUSE_BEFORE_PROPAGATING_MESSAGE_PUBLICATION_ERROR_MILLIS',
  defaultDuration: 2000
}

module.exports = {
  publishMessages: async function (config, ...args) {
    let sbClient
    try {
      sbClient = new azureServiceBus.ServiceBusClient(connectionString)
      const sender = sbClient.createSender(config.destinationName)
      const messages = await config.fn(config.context, ...args)
      await sender.sendMessages(messages)
      await sender.close()
    } catch (err) {
      // Sleep before error propgation to allow time for
      // recovery from transient errors.
      await sleep(pauseBeforePropagatingErrorConfig)
    } finally {
      await sbClient?.close()
    }
  }
}
