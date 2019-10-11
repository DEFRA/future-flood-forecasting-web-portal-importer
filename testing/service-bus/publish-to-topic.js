// Code adapted from https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-nodejs-how-to-use-topics-subscriptions-new-package

const { ServiceBusClient } = require('@azure/service-bus')

const connectionString = process.env['AzureWebJobsServiceBus']
const topicName = process.env['AZURE_SERVICE_BUS_TOPIC']

async function main () {
  const sbClient = ServiceBusClient.createFromConnectionString(connectionString)
  const topicClient = sbClient.createTopicClient(topicName)
  const sender = topicClient.createSender()

  try {
    const message = {
      body: process.env['AZURE_SERVICE_BUS_TEST_MESSAGE'],
      label: `test`
    }
    console.log(`Sending message: ${message.body}`)
    await sender.send(message)
    await topicClient.close()
  } catch (err) {
    console.log('Error occurred: ', err)
  } finally {
    await sbClient.close()
  }
}

main()
