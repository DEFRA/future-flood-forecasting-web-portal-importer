export default async function (context, message) {
  if (message.constructor.name === 'Object') {
    context.log(`Replaying ${JSON.stringify(message)}`)
  }
  context.bindings.importFromFews = message
}
