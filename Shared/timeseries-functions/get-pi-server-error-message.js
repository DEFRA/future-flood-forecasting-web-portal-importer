import { pipeline, Readable, Transform } from 'stream'
import { promisify } from 'util'
const pipe = promisify(pipeline)

export default async function (context, err) {
  let errorMessage
  if (err && ((err.response && err.response.data) || err.message)) {
    errorMessage = await getErrorMessageFromError(err)
  }
  return errorMessage
}

async function getErrorMessageFromError (error) {
  let errorDetails
  let errorMessage = error.message

  if (error.response && error.response.data) {
    errorDetails = await getErrorDetailsFromErrorResponse(error.response)
    errorMessage = `${error.message} (${errorDetails})`
  }

  return errorMessage
}

async function getErrorDetailsFromErrorResponse (errorResponse) {
  let errorDetails
  if (errorResponse && errorResponse.data instanceof Readable) {
    errorDetails = await getErrorDetailsFromStream(errorResponse.data)
    // Replace the response data with the details received from the stream
    // as the stream can only be read once.
    errorResponse.data = errorDetails
  } else {
    errorDetails = errorResponse.data
  }
  return errorDetails
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
