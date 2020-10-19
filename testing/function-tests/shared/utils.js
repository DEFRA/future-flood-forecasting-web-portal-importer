const { Readable } = require('stream')

module.exports = {
  objectToStream: async function (object) {
    // Indent the object to be converted to a stream to test that minification functions as expected.
    const stream = await bufferToStream(Buffer.from(typeof object === 'string' ? object : JSON.stringify(object, null, 2)))
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
