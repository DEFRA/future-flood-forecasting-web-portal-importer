// Code adapted from https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-nodejs-how-to-use-queues-new-package
const { ServiceBusClient } = require('@azure/service-bus')
const connectionString = process.env.AzureWebJobsServiceBus

module.exports = {
  publishMessages: async function (config, ...args) {
    const sbClient = new ServiceBusClient(connectionString)
    const sender = sbClient.createSender(config.destinationName)
    try {
      const messages = await config.fn(config.context, ...args)
      await sender.sendMessages(messages)
      await sender.close()
    } catch (err) {
      console.log('Error occurred: ', err)
    } finally {
      await sbClient.close()
    }
  }
}
