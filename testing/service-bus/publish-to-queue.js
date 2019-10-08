// Code adapted from https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-nodejs-how-to-use-queues-new-package

const { ServiceBusClient } = require('@azure/service-bus')

const connectionString = process.env['AzureWebJobsServiceBus']
const queueName = process.env['AZURE_SERVICE_BUS_QUEUE']

async function main () {
  const sbClient = ServiceBusClient.createFromConnectionString(connectionString)
  const queueClient = sbClient.createQueueClient(queueName)
  const sender = queueClient.createSender()

  try {
    const message = {
      body: process.env['AZURE_SERVICE_BUS_TEST_MESSAGE'],
      label: `test`
    }
    console.log(`Sending message: ${message.body}`)
    await sender.send(message)
    await queueClient.close()
  } catch (err) {
    console.log('Error occurred: ', err)
  } finally {
    await sbClient.close()
  }
}

main()
