const { Readable } = require('stream')

module.exports = {
  objectToStream: async function (object) {
    const stream = await bufferToStream(Buffer.from(JSON.stringify(object)))
    return stream
  }
}

// Adapted from https://stackoverflow.com/questions/47089230/how-to-convert-buffer-to-stream-in-nodejs
async function bufferToStream (buffer) {
  const readable = new Readable({
    read () {
      this.push(buffer)
      this.push(null)
    }
  })
  return readable
}
