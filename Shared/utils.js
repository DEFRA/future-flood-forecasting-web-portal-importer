const axios = require('axios')
const JSONStream = require('jsonstream-next')
const TimeseriesStagingError = require('./timeseries-functions/timeseries-staging-error')
const pino = require('pino')
const { pipeline, Transform } = require('stream')
const { createGzip } = require('zlib')
const { promisify } = require('util')
const pipe = promisify(pipeline)
const moment = require('moment')

const LATEST = 'latest'
const PREVIOUS = 'previous'

const nonProductionLoggerOptions = {
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true
    }
  }
}
const loggerOptions =
  process.env.NODE_ENV === 'development' || process.env.NODE_ENV === 'test'
    ? nonProductionLoggerOptions
    : {}

const logger = pino(loggerOptions)

const self = module.exports = {
  isBoolean: function (value) {
    if (typeof (value) === 'boolean') {
      return true
    } else if (typeof (value) === 'string') {
      return !!value.match(/^true|false$/i)
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
      return getAbsoluteIntegerForNonZeroOffsetInternal(context, offset, taskRunData)
    } else {
      context.log('Non-zero offset required.')
      return null
    }
  },
  getEnvironmentVariableAsPositiveIntegerInRange: function (config) {
    let environmentVariableAsInteger = self.getEnvironmentVariableAsAbsoluteInteger(config.environmentVariableName)
    const loggingFunction = config.context ? config.context.log.warn.bind(config.context) : self.logger.warn.bind(self.logger)
    if (!isNumericEnvironmentVariableRangeDefined(config, loggingFunction)) {
      environmentVariableAsInteger = undefined
    }

    if (Number.isInteger(Number(environmentVariableAsInteger)) &&
        (environmentVariableAsInteger < config.minimum ||
         environmentVariableAsInteger > config.maximum)) {
      environmentVariableAsInteger = undefined
      loggingFunction(`Ignoring ${config.environmentVariableName} - value must be between ${config.minimum} and ${config.maximum}`)
    }
    return environmentVariableAsInteger
  },
  getEnvironmentVariableAsBoolean: function (environmentVariableName) {
    let environmentVariableAsBoolean
    if (self.isBoolean(process.env[environmentVariableName])) {
      environmentVariableAsBoolean = JSON.parse(process.env[environmentVariableName])
    }
    return environmentVariableAsBoolean
  },
  logObsoleteTaskRunMessage: function (context, taskRunData) {
    context.log.warn(
      `Ignoring message for ${taskRunData.sourceDetails} completed on ${taskRunData.taskRunCompletionTime}` +
      ` - ${taskRunData.latestTaskRunId} completed on ${taskRunData.latestTaskRunCompletionTime} is the latest task run for workflow ${taskRunData.workflowId}`
    )
  },
  addLatestTaskRunCompletionPropertiesFromQueryResultToTaskRunData: function (taskRunData, result) {
    addTaskRunCompletionPropertiesFromQueryResultToTaskRunData(taskRunData, result, LATEST, addFallbackLatestTaskRunCompletionPropertiesToTaskRunData)
  },
  addPreviousTaskRunCompletionPropertiesFromQueryResultToTaskRunData: function (taskRunData, result) {
    addTaskRunCompletionPropertiesFromQueryResultToTaskRunData(taskRunData, result, PREVIOUS, addFallbackPreviousTaskRunCompletionPropertiesToTaskRunData)
  },
  logMessageForTaskRunPlotOrFilter: function (context, taskRunData, prefix, suffix) {
    context.log(`${prefix} for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) ${suffix || ''}`)
  },
  // durationType parameters must be an object of the folowing form:
  // {
  //    environmentVariableName: '<<Environment variable name specifying a numeric duration in milliseconds>>',
  //    defaultDuration: <<default duration in milliseconds if the environment variable is not present>>
  //  }
  getDuration: function (durationType) {
    return self.getEnvironmentVariableAsAbsoluteInteger(durationType.environmentVariableName) || durationType.defaultDuration
  },
  sleep: async function (context, durationType) {
    const duration = self.getDuration(durationType)
    context.log(`Sleeping for ${duration} millisecond(s)`)
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        resolve()
      }, duration)
    })
  },
  logger
}

function addTaskRunCompletionPropertiesFromQueryResultToTaskRunData (taskRunData, result, propertyNamePrefix, fallbackFunction) {
  if (result.recordset && result.recordset[0] && result.recordset[0][`${propertyNamePrefix}_staged_task_run_id`]) {
    taskRunData[`${propertyNamePrefix}TaskRunId`] = result.recordset[0][`${propertyNamePrefix}_staged_task_run_id`]
    taskRunData[`${propertyNamePrefix}TaskRunCompletionTime`] =
      moment(result.recordset[0][`${propertyNamePrefix}_staged_task_completion_time`]).toISOString()
  } else {
    fallbackFunction(taskRunData)
  }
}

function addFallbackLatestTaskRunCompletionPropertiesToTaskRunData (taskRunData) {
  taskRunData.latestTaskRunId = taskRunData.taskRunId
  taskRunData.latestTaskRunCompletionTime = taskRunData.taskRunCompletionTime
}

function addFallbackPreviousTaskRunCompletionPropertiesToTaskRunData (taskRunData) {
  taskRunData.previousTaskRunCompletionTime = null // task run not yet present in db
}

function getAbsoluteIntegerForNonZeroOffsetInternal (context, offset, taskRunData) {
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

function isNumericEnvironmentVariableRangeDefined (config, loggingFunction) {
  return isInteger(config.environmentVariableName, config.minimum, loggingFunction) &&
         isInteger(config.environmentVariableName, config.maximum, loggingFunction)
}

function isInteger (label, value, loggingFunction) {
  const returnValue = Number.isInteger(Number(value))
  if (!returnValue) {
    loggingFunction(`Ignoring ${label} - minimum value must be specified`)
  }
  return returnValue
}

// Set a default timeout for all PI Server calls in case the PI Server is online
// but unresponsive.
axios.defaults.timeout = self.getEnvironmentVariableAsAbsoluteInteger('PI_SERVER_CALL_TIMEOUT') || 60000
