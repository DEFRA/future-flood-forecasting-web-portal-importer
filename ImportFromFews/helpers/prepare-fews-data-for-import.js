const doIfMaximumDelayForPiServerIndexingIsNotExceeded =
  require('../../Shared/timeseries-functions/do-if-maximum-delay-for-pi-server-indexing-is-not-exceeded')
const PartialFewsDataError = require('../../Shared/message-replay/partial-fews-data-error')
const JSONStream = require('jsonstream-next')
const { pipeline, Transform } = require('stream')
const { createGzip } = require('zlib')
const { promisify } = require('util')
const pipe = promisify(pipeline)

module.exports = async function (context, taskRunData, jsonStream) {
  const buffers = []
  let buffersLength = 0

  const preStringifyObjectTransform = new Transform({
    // Object mode is required to manipulate JSON.
    objectMode: true,
    transform: async (object, encoding, done) => {
      try {
        await checkForMissingEventsIfNeeded(context, taskRunData, object)
        done(null, [object.key, object.value])
      } catch (err) {
        done(err)
      }
    }
  })

  const byteArrayTransform = new Transform({
    transform: (chunk, encoding, done) => {
      buffers.push(chunk)
      buffersLength += chunk.length
      done()
    }
  })

  const transforms = { preStringifyObjectTransform, byteArrayTransform }
  await runPipe(jsonStream, transforms)
  return Buffer.concat(buffers, buffersLength)
}
async function runPipe (jsonStream, transforms) {
  // Check for missing events if required before applying minification and gzip compression.
  // Minification is achieved using a stream compatible version of JSON.stringify(JSON.parse(jsonString)).
  const gzip = createGzip()
  await pipe(
    jsonStream,
    // Emit keys and values from the stream.
    JSONStream.parse('$*'),
    // Check for missing events and transform the keys and values into the form required by JSONStream.stringifyObject
    await transforms.preStringifyObjectTransform,
    // Minify the contents of the stream through the removal of new lines and use of
    // JSON.stringify with no indentation.
    JSONStream.stringifyObject('{', ',', '}', 0),
    gzip,
    transforms.byteArrayTransform
  )
}

async function checkForMissingEventsIfNeeded (context, taskRunData, fewsData) {
  const noActionTakenMessage = `Skipping missing event detection for ${taskRunData.sourceDetails}`

  await doIfMaximumDelayForPiServerIndexingIsNotExceeded(
    { fn: checkForMissingEvents, context, taskRunData, noActionTakenMessage }, fewsData
  )
}

async function checkForMissingEvents (context, taskRunData, fewsData) {
  // Missing events occur when timeSeries header data has an empty array of associated
  // events rather than no associated events. Missing events can be an indication that
  // PI Server indexing for a task run has not completed yet, so prepare to schedule
  // replay of the message.
  if (fewsData.key === 'timeSeries' && fewsData.value.filter(data => data?.events?.length === 0).length > 0) {
    throw new PartialFewsDataError(
      context,
      taskRunData.message,
      `Missing events detected for ${taskRunData.sourceDetails} - preparing to schedule message replay`
    )
  } else if (fewsData.key === 'timeSeries') {
    context.log(`No missing events detected for ${taskRunData.sourceDetails}`)
  }
}
