import fs from 'fs'
import axios from 'axios'
import TimeseriesStagingError from './timeseries-functions/timeseries-staging-error.js'
import pino from 'pino'
import moment from 'moment'

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

export const logger = pino(loggerOptions)

export const isBoolean = function (value) {
  if (typeof (value) === 'boolean') {
    return true
  } else if (typeof (value) === 'string') {
    return !!value.match(/^true|false$/i)
  } else {
    return false
  }
}

export const getEnvironmentVariableAsAbsoluteInteger = function (environmentVariableName) {
  let environmentVariableAsInteger
  const parsedEnvironmentVariable = Number(process.env[environmentVariableName])
  if (Number.isInteger(parsedEnvironmentVariable)) {
    environmentVariableAsInteger = Math.abs(parsedEnvironmentVariable)
  }
  return environmentVariableAsInteger
}

export const getAbsoluteIntegerForNonZeroOffset = function (context, offset, taskRunData) {
  if (offset && offset !== 0) {
    return getAbsoluteIntegerForNonZeroOffsetInternal(context, offset, taskRunData)
  } else {
    context.log('Non-zero offset required.')
    return null
  }
}

export const getEnvironmentVariableAsPositiveIntegerInRange = function (config) {
  let environmentVariableAsInteger = getEnvironmentVariableAsAbsoluteInteger(config.environmentVariableName)
  const loggingFunction = config.context ? config.context.log.warn.bind(config.context) : logger.warn.bind(logger)
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
}

export const getEnvironmentVariableAsBoolean = function (environmentVariableName) {
  let environmentVariableAsBoolean
  if (isBoolean(process.env[environmentVariableName])) {
    environmentVariableAsBoolean = JSON.parse(process.env[environmentVariableName])
  }
  return environmentVariableAsBoolean
}

export const logObsoleteTaskRunMessage = function (context, taskRunData) {
  context.log.warn(
    `Ignoring message for ${taskRunData.sourceDetails} completed on ${taskRunData.taskRunCompletionTime}` +
    ` - ${taskRunData.latestTaskRunId} completed on ${taskRunData.latestTaskRunCompletionTime} is the latest task run for workflow ${taskRunData.workflowId}`
  )
}

export const addLatestTaskRunCompletionPropertiesFromQueryResultToTaskRunData = function (taskRunData, result) {
  addTaskRunCompletionPropertiesFromQueryResultToTaskRunData(taskRunData, result, LATEST, addFallbackLatestTaskRunCompletionPropertiesToTaskRunData)
}

export const addPreviousTaskRunCompletionPropertiesFromQueryResultToTaskRunData = function (taskRunData, result) {
  addTaskRunCompletionPropertiesFromQueryResultToTaskRunData(taskRunData, result, PREVIOUS, addFallbackPreviousTaskRunCompletionPropertiesToTaskRunData)
}

export const logMessageForTaskRunPlotOrFilter = function (context, taskRunData, prefix, suffix) {
  context.log(`${prefix} for ${taskRunData.sourceTypeDescription} ${taskRunData.sourceId} of task run ${taskRunData.taskRunId} (workflow ${taskRunData.workflowId}) ${suffix || ''}`)
}

export const loadJsonFile = function (file) {
  return JSON.parse(fs.readFileSync(file))
}

export const getDuration = function (durationType) {
  return getEnvironmentVariableAsAbsoluteInteger(durationType.environmentVariableName) || durationType.defaultDuration
}

export const sleep = async function (context, durationType) {
  const duration = getDuration(durationType)
  context.log(`Sleeping for ${duration} millisecond(s)`)
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve()
    }, duration)
  })
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
axios.defaults.timeout = getEnvironmentVariableAsAbsoluteInteger('PI_SERVER_CALL_TIMEOUT') || 60000
