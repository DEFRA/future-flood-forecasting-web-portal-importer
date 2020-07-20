const { pipeline, Readable, Transform } = require('stream')
const { promisify } = require('util')
const pipe = promisify(pipeline)

module.exports = async function (context, err) {
  let errorMessage
  if (err && ((err.response && err.response.data) || err.message)) {
    errorMessage = err.message
    if (err.response && err.response.data) {
      let errorDetails
      if (err.response.data instanceof Readable) {
        errorDetails = await getErrorDetailsFromStream(err.response.data)
      } else {
        errorDetails = err.response.data
      }
      errorMessage = `${err.message} (${errorDetails})`
    }
  }
  return errorMessage
}

async function getErrorDetailsFromStream (stream) {
  let errorDetails = ''
  const stringTransform = new Transform({
    transform: (chunk, encoding, done) => {
      errorDetails += chunk.toString()
      done()
    }
  })
  await pipe(stream, stringTransform)
  return errorDetails
}
