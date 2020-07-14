const { createGzip } = require('zlib')
const { pipeline, Transform } = require('stream')
const { promisify } = require('util')
const pipe = promisify(pipeline)

module.exports = {
  isBoolean: function (value) {
    if (typeof (value) === 'boolean') {
      return true
    } else if (typeof (value) === 'string') {
      return !!value.match(/true|false/i)
    } else {
      return false
    }
  },
  gzip: async function (stream) {
    const gzip = createGzip()
    const buffers = []
    let buffersLength = 0

    const byteArrayTransform = new Transform({
      transform: (chunk, encoding, done) => {
        buffers.push(chunk)
        buffersLength += chunk.length
        done()
      }
    })

    await pipe(stream, gzip, byteArrayTransform)
    return Buffer.concat(buffers, buffersLength)
  },
  getEnvironmentVariableAsInteger: function (environmentVariableName) {
    let environmentVariableAsInteger
    if (Number.isInteger(process.env[environmentVariableName])) {
      environmentVariableAsInteger = Number(environmentVariableName)
    }
    return environmentVariableAsInteger
  }
}
