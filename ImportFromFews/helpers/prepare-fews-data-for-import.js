const JSONStream = require('jsonstream-next')
const { pipeline, Transform } = require('stream')
const { createGzip } = require('zlib')
const { promisify } = require('util')
const pipe = promisify(pipeline)

async function runPipe (jsonStream, transforms) {
  // Filter data if required before applying minification and gzip compression.
  // Minification is achieved using a stream compatible version of JSON.stringify(JSON.parse(jsonString)).
  const gzip = createGzip()
  await pipe(
    jsonStream,
    // Emit keys and values from the stream.
    JSONStream.parse('$*'),
    // Filter values from the stream if needed.
    transforms.filterTransform,
    // Transform the keys and values into the form required by JSONStream.stringifyObject
    transforms.preStringifyObjectTransform,
    // Minify the contents of the stream through the removal of new lines and use of
    // JSON.stringify with no indentation.
    JSONStream.stringifyObject('{', ',', '}', 0),
    gzip,
    transforms.byteArrayTransform
  )
}

function filterFewsData (fewsData, taskRunData) {
  // CI-373 - Filter plot based timeseries data for task runs of fluvial forecast workflows so that only:
  // - FMROP ensemble data is loaded for operational forecasts.
  // - FMRBE ensemble data is loaded for best estimate forecasts.
  // - FMRRWC ensemble data is loaded for reasonable worst case forecasts.
  //
  // This approach also filters out historical fluvial data.
  fewsData.value =
    taskRunData.csvType === 'F' && taskRunData.plotId && fewsData.key === 'timeSeries' &&
    (taskRunData.workflowId?.endsWith('_OP') || taskRunData.workflowId?.endsWith('_BE') || taskRunData.workflowId?.endsWith('_RWC'))
      ? fewsData.value.filter(v => v?.header?.ensembleId === `FMR${taskRunData.workflowId.split('_')[taskRunData.workflowId.split('_').length - 1]}`)
      : fewsData.value
}

module.exports = async function (jsonStream, taskRunData) {
  const buffers = []
  let buffersLength = 0

  const filterTransform = new Transform({
    // Object mode is required to manipulate JSON.
    objectMode: true,
    transform: (object, encoding, done) => {
      filterFewsData(object, taskRunData)
      done(null, object)
    }
  })

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

  const transforms = { filterTransform, preStringifyObjectTransform, byteArrayTransform }
  await runPipe(jsonStream, transforms)
  return Buffer.concat(buffers, buffersLength)
}
