const sql = require('mssql')
const { getEnvironmentVariableAsAbsoluteInteger, isBoolean } = require('./utils')
const { logger } = require('defra-logging-facade')
const hostJson = require('../host.json')

module.exports = function () {
  // Configure database connectivity using:
  // - some mssql defaults
  // - some custom defaults
  // - selected environment variable based customisation.
  const numericEnvironmentVariables = getNumericEnvironmentVariables()
  const maxConcurrentCalls = hostJson.extensions.serviceBus.messageHandlerOptions.maxConcurrentCalls

  const config = {
    user: process.env.SQLDB_USER,
    password: process.env.SQLDB_PASSWORD,
    server: process.env.SQLDB_SERVER,
    database: process.env.SQLDB_DATABASE,
    requestTimeout: numericEnvironmentVariables.requestTimeout || 60000,
    options: {
      // Do not raise an error during loss of precision as this must be disabled for certain operations
      // (see https://docs.microsoft.com/en-us/sql/t-sql/statements/set-numeric-roundabort-transact-sql?view=sql-server-ver15).
      enableNumericRoundabort: false
    },
    pool: {
      min: numericEnvironmentVariables.minPooledConnections || maxConcurrentCalls + 1,
      max: numericEnvironmentVariables.maxPooledConnections || maxConcurrentCalls * 2,
      propagateCreateError: false
    }
  }

  addOptionalConfig(config, numericEnvironmentVariables)
  this.pool = new sql.ConnectionPool(config)

  // To catch critical pool failures
  this.pool.on('error', err => {
    logger.error(err)
    throw err
  })
}

function addOptionalConfig (config, numericEnvironmentVariables) {
  !Object.is(numericEnvironmentVariables.port, undefined) && (config.port = numericEnvironmentVariables.port)
  !Object.is(numericEnvironmentVariables.connectionTimeout, undefined) && (config.connectionTimeout = numericEnvironmentVariables.connectionTimeout)
  !Object.is(numericEnvironmentVariables.maxRetriesOnTransientErrors, undefined) && (config.options.maxRetriesOnTransientErrors = numericEnvironmentVariables.maxRetriesOnTransientErrors)
  !Object.is(numericEnvironmentVariables.packetSize, undefined) && (config.options.packetSize = numericEnvironmentVariables.packetSize)
  !Object.is(numericEnvironmentVariables.acquireTimeoutMillis, undefined) && (config.pool.acquireTimeoutMillis = numericEnvironmentVariables.acquireTimeoutMillis)
  !Object.is(numericEnvironmentVariables.createTimeoutMillis, undefined) && (config.pool.createTimeoutMillis = numericEnvironmentVariables.createTimeoutMillis)
  !Object.is(numericEnvironmentVariables.destroyTimeoutMillis, undefined) && (config.pool.destroyTimeoutMillis = numericEnvironmentVariables.destroyTimeoutMillis)
  !Object.is(numericEnvironmentVariables.idleTimeoutMillis, undefined) && (config.pool.idleTimeoutMillis = numericEnvironmentVariables.idleTimeoutMillis)
  !Object.is(numericEnvironmentVariables.reapIntervalMillis, undefined) && (config.pool.reapIntervalMillis = numericEnvironmentVariables.reapIntervalMillis)
  !Object.is(numericEnvironmentVariables.createRetryIntervalMillis, undefined) && (config.pool.createRetryIntervalMillis = numericEnvironmentVariables.createRetryIntervalMillis)

  if (isBoolean(process.env.SQLDB_ABORT_TRANSACTION_ON_ERROR)) {
    config.options.abortTransactionOnError = JSON.parse(process.env.SQLDB_ABORT_TRANSACTION_ON_ERROR)
  }
}

function getNumericEnvironmentVariables () {
  const numericEnvironmentVariables = {
    port: getEnvironmentVariableAsAbsoluteInteger('SQLDB_PORT'),
    connectionTimeout: getEnvironmentVariableAsAbsoluteInteger('SQLDB_CONNECTION_TIMEOUT_MILLIS'),
    requestTimeout: getEnvironmentVariableAsAbsoluteInteger('SQLDB_REQUEST_TIMEOUT_MILLIS'),
    maxRetriesOnTransientErrors: getEnvironmentVariableAsAbsoluteInteger('SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS'),
    packetSize: getEnvironmentVariableAsAbsoluteInteger('SQLDB_PACKET_SIZE'),
    maxPooledConnections: getEnvironmentVariableAsAbsoluteInteger('SQLDB_MAX_POOLED_CONNECTIONS'),
    minPooledConnections: getEnvironmentVariableAsAbsoluteInteger('SQLDB_MIN_POOLED_CONNECTIONS'),
    acquireTimeoutMillis: getEnvironmentVariableAsAbsoluteInteger('SQLDB_ACQUIRE_TIMEOUT_MILLIS'),
    createTimeoutMillis: getEnvironmentVariableAsAbsoluteInteger('SQLDB_CREATE_TIMEOUT_MILLIS'),
    destroyTimeoutMillis: getEnvironmentVariableAsAbsoluteInteger('SQLDB_DESTROY_TIMEOUT_MILLIS'),
    idleTimeoutMillis: getEnvironmentVariableAsAbsoluteInteger('SQLDB_IDLE_TIMEOUT_MILLIS'),
    reapIntervalMillis: getEnvironmentVariableAsAbsoluteInteger('SQLDB_REAP_INTERVAL_MILLIS'),
    createRetryIntervalMillis: getEnvironmentVariableAsAbsoluteInteger('SQLDB_CREATE_RETRY_INTERVAL_MILLIS')
  }
  return numericEnvironmentVariables
}
