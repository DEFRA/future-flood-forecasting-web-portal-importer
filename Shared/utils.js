const TimeseriesStagingError = require('../ImportFromFews/helpers/timeseries-staging-error')
const { pipeline, Transform } = require('stream')
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
  getEnvironmentVariableAsAbsoluteInteger: function (environmentVariableName) {
    let environmentVariableAsInteger
    if (Number.isInteger(process.env[environmentVariableName])) {
      environmentVariableAsInteger = Math.abs(Number(process.env[environmentVariableName]))
    }
    return environmentVariableAsInteger
  },
  getOffsetAsAbsoluteInteger: function (offset, taskRunData) {
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
  }
}
