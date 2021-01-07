const sql = require('mssql')
const { getEnvironmentVariableAsAbsoluteInteger, isBoolean } = require('./utils')
const { logger } = require('defra-logging-facade')

module.exports = function () {
  // Configure database connectivity using:
  // - some mssql defaults
  // - some custom defaults
  // - selected environment variable based customisation.
  const abortTransactionOnError = isBoolean(process.env.SQLDB_ABORT_TRANSACTION_ON_ERROR) ? process.env.SQLDB_ABORT_TRANSACTION_ON_ERROR : false
  const port = getEnvironmentVariableAsAbsoluteInteger('SQLDB_PORT')
  const connectionTimeout = getEnvironmentVariableAsAbsoluteInteger('SQLDB_CONNECTION_TIMEOUT')
  const requestTimeout = getEnvironmentVariableAsAbsoluteInteger('SQLDB_REQUEST_TIMEOUT')
  const maxRetriesOnTransientErrors = getEnvironmentVariableAsAbsoluteInteger('SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS')
  const maxPooledConnections = getEnvironmentVariableAsAbsoluteInteger('SQLDB_MAX_POOLED_CONNECTIONS')
  const minPooledConnections = getEnvironmentVariableAsAbsoluteInteger('SQLDB_MIN_POOLED_CONNECTIONS')
  const acquireTimeoutMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_ACQUIRE_TIMEOUT_MILLIS')
  const createTimeoutMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_CREATE_TIMEOUT_MILLIS')
  const destroyTimeoutMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_DESTROY_TIMEOUT_MILLIS')
  const idleTimeoutMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_IDLE_TIMEOUT_MILLIS')
  const reapIntervalMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_REAP_INTERVAL_MILLIS')
  const createRetryIntervalMillis = getEnvironmentVariableAsAbsoluteInteger('SQLDB_CREATE_RETRY_INTERVAL_MILLIS')

  const config = {
    user: process.env.SQLDB_USER,
    password: process.env.SQLDB_PASSWORD,
    server: process.env.SQLDB_SERVER,
    port: port || 1433,
    database: process.env.SQLDB_DATABASE,
    connectionTimeout: connectionTimeout || 15000,
    requestTimeout: requestTimeout || 15000,
    maxRetriesOnTransientErrors: maxRetriesOnTransientErrors || 3,
    abortTransactionOnError: abortTransactionOnError,
    pool: {
      max: maxPooledConnections || 10,
      min: minPooledConnections || 6,
      acquireTimeoutMillis: acquireTimeoutMillis || 30000,
      createTimeoutMillis: createTimeoutMillis || 30000,
      destroyTimeoutMillis: destroyTimeoutMillis || 5000,
      idleTimeoutMillis: idleTimeoutMillis || 30000,
      reapIntervalMillis: reapIntervalMillis || 1000,
      createRetryIntervalMillis: createRetryIntervalMillis || 200
    }
  }

  this.pool = new sql.ConnectionPool(config)

  // To catch critical pool failures
  this.pool.on('error', err => {
    logger.error(err)
    throw err
  })
}
