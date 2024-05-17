const { publishMessages } = require('../../Shared/service-bus-helper')

const destinationName = process.env.AZURE_SERVICE_BUS_QUEUE

function getMessages () {
  const message = {
    body: process.env.AZURE_SERVICE_BUS_TEST_MESSAGE,
    label: 'test'
  }
  console.log(`Sending message: ${message.body}`)
  return [message]
}

async function main () {
  publishMessages({
    destinationName,
    fn: getMessages
  })
}

main()
