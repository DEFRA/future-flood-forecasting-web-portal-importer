const TimeseriesStagingError = require('./timeseries-functions/timeseries-staging-error')
const { pipeline, Transform } = require('stream')
const JSONStream = require('jsonstream-next')
const { createGzip } = require('zlib')
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
  minifyAndGzip: async function (jsonStream) {
    const gzip = createGzip()
    const buffers = []
    let buffersLength = 0

    const preStringifyObjectTransform = new Transform({
      // Object mode is required to manipulate JSON.
      objectMode: true,
      transform: (object, encoding, done) => {
        done(null, [object.key, object.value])
      }
    })

    const byteArrayTransform = new Transform({
      transform: (chunk, encoding, done) => {
        buffers.push(chunk)
        buffersLength += chunk.length
        done()
      }
    })

    // Minification is achieved using a stream compatible version of JSON.stringify(JSON.parse(jsonString)).
    await pipe(
      jsonStream,
      // Emit keys and values from the stream.
      JSONStream.parse('$*'),
      // Transform the keys and values into the form required by JSONStream.stringifyObject
      preStringifyObjectTransform,
      // Minify the contents of the stream through the removal of new lines and use of
      // JSON.stringify with no indentation.
      JSONStream.stringifyObject('{', ',', '}', 0),
      gzip,
      byteArrayTransform
    )

    return Buffer.concat(buffers, buffersLength)
  },
  getEnvironmentVariableAsAbsoluteInteger: function (environmentVariableName) {
    let environmentVariableAsInteger
    const parsedEnvironmentVariable = Number(process.env[environmentVariableName])
    if (Number.isInteger(parsedEnvironmentVariable)) {
      environmentVariableAsInteger = Math.abs(parsedEnvironmentVariable)
    }
    return environmentVariableAsInteger
  },
  getAbsoluteIntegerForNonZeroOffset: function (context, offset, taskRunData) {
    if (offset && offset !== 0) {
      let offsetInteger
      if (Number.isInteger(offset)) {
        offsetInteger = Math.abs(Number(offset))
      } else {
        const errorDescription = `Unable to return an integer for an offset value: ${offset}`

        const errorData = {
          sourceId: taskRunData.sourceId,
          sourceType: taskRunData.sourceType,
          csvError: true,
          csvType: taskRunData.csvType || 'U',
          fewsParameters: null,
          timeseriesHeaderId: taskRunData.timeseriesHeaderId,
          payload: taskRunData.message,
          description: errorDescription
        }
        throw new TimeseriesStagingError(errorData, errorDescription)
      }
      return offsetInteger
    } else {
      context.log('Non-zero offset required.')
      return null
    }
  }
}
