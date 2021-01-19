const sql = require('mssql')
const { getEnvironmentVariableAsAbsoluteInteger, isBoolean } = require('./utils')
const { logger } = require('defra-logging-facade')
const hostJson = require('../host.json')

module.exports = function () {
  // Configure database connectivity using:
  // - some mssql defaults
  // - some custom defaults
  // - selected environment variable based customisation.
  const port = getEnvironmentVariableAsAbsoluteInteger('SQLDB_PORT')
  const connectionTimeout = getEnvironmentVariableAsAbsoluteInteger('SQLDB_CONNECTION_TIMEOUT_MILLIS')
  const requestTimeout = getEnvironmentVariableAsAbsoluteInteger('SQLDB_REQUEST_TIMEOUT_MILLIS')
  const maxRetriesOnTransientErrors = getEnvironmentVariableAsAbsoluteInteger('SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS')
  const packetSize = getEnvironmentVariableAsAbsoluteInteger('SQLDB_PACKET_SIZE')
  const maxPooledConnections = getEnvironmentVariableAsAbsoluteInteger('SQLDB_MAX_POOLED_CONNECTIONS')
  const minPooledConnections = getEnvironmentVariableAsAbsoluteInteger('SQLDB_MIN_POOLED_CONNECTIONS')
  const acquireTimeoutMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_ACQUIRE_TIMEOUT_MILLIS')
  const createTimeoutMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_CREATE_TIMEOUT_MILLIS')
  const destroyTimeoutMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_DESTROY_TIMEOUT_MILLIS')
  const idleTimeoutMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_IDLE_TIMEOUT_MILLIS')
  const reapIntervalMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_REAP_INTERVAL_MILLIS')
  const createRetryIntervalMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_CREATE_RETRY_INTERVAL_MILLIS')

  const maxConcurrentCalls = hostJson.extensions.serviceBus.messageHandlerOptions.maxConcurrentCalls

  const config = {
    user: process.env.SQLDB_USER,
    password: process.env.SQLDB_PASSWORD,
    server: process.env.SQLDB_SERVER,
    database: process.env.SQLDB_DATABASE,
    requestTimeout: requestTimeout || 60000,
    maxRetriesOnTransientErrors: 20,
    pool: {
      min: minPooledConnections || maxConcurrentCalls + 1,
      max: maxPooledConnections || maxConcurrentCalls * 2,
      propagateCreateError: false
    }
  }

  if (port) {
    config.port = port
  }

  if (connectionTimeout) {
    config.connectionTimeout = connectionTimeout
  }

  if (maxRetriesOnTransientErrors) {
    config.maxRetriesOnTransientErrors = maxRetriesOnTransientErrors
  }

  if (packetSize) {
    config.packetSize = packetSize
  }

  if (isBoolean(process.env.SQLDB_ABORT_TRANSACTION_ON_ERROR)) {
    config.abortTransactionOnError = JSON.parse(process.env.SQLDB_ABORT_TRANSACTION_ON_ERROR)
  }

  if (acquireTimeoutMillis) {
    config.pool.acquireTimeoutMillis = acquireTimeoutMillis
  }

  if (createTimeoutMillis) {
    config.pool.createTimeoutMillis = createTimeoutMillis
  }

  if (destroyTimeoutMillis) {
    config.pool.destroyTimeoutMillis = destroyTimeoutMillis
  }

  if (idleTimeoutMillis) {
    config.pool.idleTimeoutMillis = idleTimeoutMillis
  }

  if (reapIntervalMillis) {
    config.pool.reapIntervalMillis = reapIntervalMillis
  }

  if (createRetryIntervalMillis) {
    config.pool.createRetryIntervalMillis = createRetryIntervalMillis
  }

  this.pool = new sql.ConnectionPool(config)

  // To catch critical pool failures
  this.pool.on('error', err => {
    logger.error(err)
    throw err
  })
}
