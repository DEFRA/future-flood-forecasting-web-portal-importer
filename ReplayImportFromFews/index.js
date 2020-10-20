module.exports = async function (context, message) {
  context.log(`Replaying ${message}`)
  context.bindings.importFromFews = message
}
