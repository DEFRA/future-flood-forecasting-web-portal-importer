const sql = require('mssql')
const { getEnvironmentVariableAsBoolean, getEnvironmentVariableAsPositiveIntegerInRange, logger } = require('./utils')
const hostJson = require('../host.json')

module.exports = function () {
  // Configure database connectivity using:
  // - some mssql defaults
  // - some custom defaults
  // - selected environment variable based customisation.
  const numericEnvironmentVariables = getNumericEnvironmentVariables()
  const booleanEnvironmentVariables = getBooleanEnvironmentVariables()
  const maxConcurrentCalls = Math.max(1, Math.min(hostJson.extensions.serviceBus.maxConcurrentCalls, 10))

  const config = {
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
      max: numericEnvironmentVariables.maxPooledConnections || maxConcurrentCalls * 2
    }
  }

  addAuthenticationConfig(config, booleanEnvironmentVariables)
  addOptionalConfig(config, numericEnvironmentVariables, booleanEnvironmentVariables)
  this.pool = new sql.ConnectionPool(config)

  // To catch critical pool failures
  this.pool.on('error', err => {
    logger.error(err)
    throw err
  })
}

function addAuthenticationConfig (config, booleanEnvironmentVariables) {
  if (booleanEnvironmentVariables.authenticateWithMsi &&
      process.env.MSI_ENDPOINT &&
      process.env.MSI_SECRET) {
    logger.info('Configuring MSI app service authentication')

    config.authentication = {
      type: 'azure-active-directory-msi-app-service',
      options: {
        msiEndpoint: process.env.MSI_ENDPOINT,
        msiSecret: process.env.MSI_SECRET
      }
    }
  } else {
    logger.info('Configuring default authentication')
    config.user = process.env.SQLDB_USER
    config.password = process.env.SQLDB_PASSWORD
  }
}

function addOptionalConfig (config, numericEnvironmentVariables, booleanEnvironmentVariables) {
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

  !Object.is(booleanEnvironmentVariables.abortTransactionOnError, undefined) && (config.options.abortTransactionOnError = booleanEnvironmentVariables.abortTransactionOnError)
  !Object.is(booleanEnvironmentVariables.trustServerCertificate, undefined) && (config.options.trustServerCertificate = booleanEnvironmentVariables.trustServerCertificate)
  !Object.is(booleanEnvironmentVariables.propagateCreateError, undefined) && (config.pool.propagateCreateError = booleanEnvironmentVariables.propagateCreateError)
}

function getNumericEnvironmentVariables () {
  const numericEnvironmentVariables = {
    port: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_PORT', minimum: 1024, maximum: 49151 }),
    connectionTimeout: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_CONNECTION_TIMEOUT_MILLIS', minimum: 15000, maximum: 60000 }),
    requestTimeout: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_REQUEST_TIMEOUT_MILLIS', minimum: 15000, maximum: 120000 }),
    maxRetriesOnTransientErrors: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS', minimum: 3, maximum: 20 }),
    packetSize: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_PACKET_SIZE', minimum: 4096, maximum: 65536 }),
    maxPooledConnections: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_MAX_POOLED_CONNECTIONS', minimum: 1, maximum: 20 }),
    minPooledConnections: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_MIN_POOLED_CONNECTIONS', minimum: 1, maximum: 20 }),
    acquireTimeoutMillis: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_ACQUIRE_TIMEOUT_MILLIS', minimum: 5000, maximum: 120000 }),
    createTimeoutMillis: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_CREATE_TIMEOUT_MILLIS', minimum: 5000, maximum: 120000 }),
    destroyTimeoutMillis: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_DESTROY_TIMEOUT_MILLIS', minimum: 5000, maximum: 30000 }),
    idleTimeoutMillis: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_IDLE_TIMEOUT_MILLIS', minimum: 5000, maximum: 120000 }),
    reapIntervalMillis: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_REAP_INTERVAL_MILLIS', minimum: 1000, maximum: 30000 }),
    createRetryIntervalMillis: getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SQLDB_CREATE_RETRY_INTERVAL_MILLIS', minimum: 200, maximum: 5000 })
  }
  return validate(numericEnvironmentVariables)
}

function getBooleanEnvironmentVariables () {
  const booleanEnvironmentVariables = {
    abortTransactionOnError: getEnvironmentVariableAsBoolean('SQLDB_ABORT_TRANSACTION_ON_ERROR'),
    trustServerCertificate: getEnvironmentVariableAsBoolean('SQLDB_TRUST_SERVER_CERTIFICATE'),
    propagateCreateError: getEnvironmentVariableAsBoolean('SQLDB_PROPAGATE_CREATE_ERROR'),
    authenticateWithMsi: getEnvironmentVariableAsBoolean('AUTHENTICATE_WITH_MSI')
  }
  return booleanEnvironmentVariables
}

function validate (numericEnvironmentVariables) {
  if (!Object.is(numericEnvironmentVariables.minPooledConnections, undefined) && !Object.is(numericEnvironmentVariables.maxPooledConnections, undefined) && numericEnvironmentVariables.minPooledConnections > numericEnvironmentVariables.maxPooledConnections) {
    logger.warn(`Ignoring custom connection pool size - The minimum number of connections (${numericEnvironmentVariables.minPooledConnections}) cannot be greater than the maximum number of connections (${numericEnvironmentVariables.maxPooledConnections})`)
    delete numericEnvironmentVariables.minPooledConnections
    delete numericEnvironmentVariables.maxPooledConnections
  }

  if (numericEnvironmentVariables.packetSize && Math.log2(numericEnvironmentVariables.packetSize) % 1 !== 0) {
    logger.warn(`Ignoring custom packet size that is not a power of two (${numericEnvironmentVariables.packetSize})`)
    delete numericEnvironmentVariables.packetSize
  }

  return numericEnvironmentVariables
}
