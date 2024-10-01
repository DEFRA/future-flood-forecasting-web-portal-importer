import ConnectionPool from '../../../Shared/connection-pool.js'
import Context from '../mocks/defaultContext.js'
import { getEnvironmentVariableAsPositiveIntegerInRange, loadJsonFile } from '../../../Shared/utils.js'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
const hostJson = loadJsonFile('./host.json')

export const invalidEnvironmentVariableBasedConfigurationTests = () => describe('Test invalid environment variable based configuration', () => {
  let pool

  describe('Invalid environment variables', () => {
    beforeAll(async () => {
      process.env.SAMPLE_ENVIRONMENT_VARIABLE = 'a'
      process.env.SQLDB_ABORT_TRANSACTION_ON_ERROR = 'falsey'
      process.env.SQLDB_PORT = '80'
      process.env.SQLDB_CONNECTION_TIMEOUT_MILLIS = '1000'
      process.env.SQLDB_REQUEST_TIMEOUT_MILLIS = '1500'
      process.env.SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS = '0'
      process.env.SQLDB_PACKET_SIZE = '5000'
      process.env.SQLDB_MAX_POOLED_CONNECTIONS = '4'
      process.env.SQLDB_MIN_POOLED_CONNECTIONS = '5'
      process.env.SQLDB_ACQUIRE_TIMEOUT_MILLIS = '2000'
      process.env.SQLDB_CREATE_TIMEOUT_MILLIS = '2500'
      process.env.SQLDB_DESTROY_TIMEOUT_MILLIS = '3000'
      process.env.SQLDB_IDLE_TIMEOUT_MILLIS = '3500'
      process.env.SQLDB_REAP_INTERVAL_MILLIS = '500'
      process.env.SQLDB_CREATE_RETRY_INTERVAL_MILLIS = '60000'
      process.env.SQLDB_PROPAGATE_CREATE_ERROR = 'truthy'
      const connectionPool = new ConnectionPool()
      pool = connectionPool.pool
      await pool.connect()
    })

    afterAll(async () => {
      if (pool) {
        await pool.close()
      }
    })

    it('should be ignored when initialising a connection pool', async () => {
      let connection
      try {
        connection = await pool.acquire()
        expect(connection.config.options.port).not.toBe(parseInt(process.env.SQLDB_PORT))
        expect(connection.config.options.abortTransactionOnError).toBe(false)
        expect(connection.config.options.enableNumericRoundabort).toBe(false)
        expect(connection.config.options.connectTimeout).not.toBe(parseInt(process.env.SQLDB_CONNECTION_TIMEOUT_MILLIS))
        expect(connection.config.options.requestTimeout).not.toBe(parseInt(process.env.SQLDB_REQUEST_TIMEOUT_MILLIS))
        expect(connection.config.options.maxRetriesOnTransientErrors).not.toBe(parseInt(process.env.SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS))
        expect(connection.config.options.packetSize).not.toBe(parseInt(process.env.SQLDB_PACKET_SIZE))
        // If the connection pool size is invalid, the default size should be configured.
        expect(pool.pool.max).toBe(hostJson.extensions.serviceBus.maxConcurrentCalls * 2)
        expect(pool.pool.min).toBe(hostJson.extensions.serviceBus.maxConcurrentCalls + 1)
        expect(pool.pool.acquireTimeoutMillis).not.toBe(parseInt(process.env.SQLDB_ACQUIRE_TIMEOUT_MILLIS))
        expect(pool.pool.createTimeoutMillis).not.toBe(parseInt(process.env.SQLDB_CREATE_TIMEOUT_MILLIS))
        expect(pool.pool.destroyTimeoutMillis).not.toBe(parseInt(process.env.SQLDB_DESTROY_TIMEOUT_MILLIS))
        expect(pool.pool.idleTimeoutMillis).not.toBe(parseInt(process.env.SQLDB_IDLE_TIMEOUT_MILLIS))
        expect(pool.pool.reapIntervalMillis).not.toBe(parseInt(process.env.SQLDB_REAP_INTERVAL_MILLIS))
        expect(pool.pool.createRetryIntervalMillis).not.toBe(parseInt(process.env.SQLDB_CREATE_RETRY_INTERVAL_MILLIS))
      } finally {
        if (connection) {
          await pool.release(connection)
        }
      }
    })

    it('should be detected by utility functions', async () => {
      const sampleEnvironmentVariable = getEnvironmentVariableAsPositiveIntegerInRange({ environmentVariableName: 'SAMPLE_ENVIRONMENT_VARIABLE', context: new Context() })
      expect(sampleEnvironmentVariable).toBeUndefined()
    })
  })
})
