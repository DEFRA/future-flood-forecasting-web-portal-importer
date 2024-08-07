const doIfMaximumDelayForPiServerIndexingIsNotExceeded =
  require('../../Shared/timeseries-functions/do-if-maximum-delay-for-pi-server-indexing-is-not-exceeded')
const { getEnvironmentVariableAsAbsoluteInteger } = require('../../Shared/utils')
const PartialFewsDataError = require('../../Shared/message-replay/partial-fews-data-error')
const JSONStream = require('jsonstream-next')
const { pipeline, Transform } = require('stream')
const { createGzip } = require('zlib')
const { promisify } = require('util')
const pipe = promisify(pipeline)

// Delay message replay for thirty seconds by default to mitigate the risk of PI Server overload.
const MESSAGE_REPLAY_DELAY_MILLIS =
  getEnvironmentVariableAsAbsoluteInteger('CHECK_FOR_TASK_RUN_MISSING_EVENTS_DELAY_MILLIS') || 30000

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
    // Check for missing events and transform the keys and values into the form required by JSONStream.stringifyObject.
    // NOTE - Missing event processing is performed using asynchronous functions because the utility function
    // used during processng (doIfMaximumDelayForPiServerIndexingIsNotExceeded) is asynchronous.
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
  // NOTE - This function is asynchronous because it is called asynchronously by the
  // utility function doIfMaximumDelayForPiServerIndexingIsNotExceeded. This function
  // could be enhanced to include asynchronous processng (for example, to record missing
  // event details in the staging database), but this is not required currently.
  //
  // Missing events can be an indication that PI Server indexing for a task run has not completed yet.
  // If there is no events attribute or the events attribute contains an empty array, prepare to
  // schedule replay of the message.
  if (fewsData.key === 'timeSeries' && fewsData.value.filter(data => !data.events || data?.events?.length === 0).length > 0) {
    const config = {
      context,
      messageToReplay: taskRunData.message,
      replayDelayMillis: MESSAGE_REPLAY_DELAY_MILLIS,
      bindingName: 'importFromFews'
    }
    throw new PartialFewsDataError(
      config,
      `Missing events detected for ${taskRunData.sourceDetails} - preparing to schedule message replay`
    )
  } else if (fewsData.key === 'timeSeries') {
    context.log(`No missing events detected for ${taskRunData.sourceDetails}`)
  }
}
