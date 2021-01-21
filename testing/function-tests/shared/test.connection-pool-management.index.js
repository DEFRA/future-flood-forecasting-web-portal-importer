const transactionHelper = require('../../../Shared/transaction-helper')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')

module.exports = describe('Test connection pool management', () => {
  let pool

  describe('A connection pool', () => {
    beforeAll(async () => {
      process.env.SQLDB_ABORT_TRANSACTION_ON_ERROR = 'true'
      process.env.SQLDB_PORT = '1433'
      process.env.SQLDB_CONNECTION_TIMEOUT_MILLIS = '20000'
      process.env.SQLDB_REQUEST_TIMEOUT_MILLIS = '30000'
      process.env.SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS = '5'
      process.env.SQLDB_PACKET_SIZE = '8192'
      process.env.SQLDB_MAX_POOLED_CONNECTIONS = '5'
      process.env.SQLDB_MIN_POOLED_CONNECTIONS = '1'
      process.env.SQLDB_ACQUIRE_TIMEOUT_MILLIS = '40000'
      process.env.SQLDB_CREATE_TIMEOUT_MILLIS = '50000'
      process.env.SQLDB_DESTROY_TIMEOUT_MILLIS = '10000'
      process.env.SQLDB_IDLE_TIMEOUT_MILLIS = '55000'
      process.env.SQLDB_REAP_INTERVAL_MILLIS = '2000'
      process.env.SQLDB_CREATE_RETRY_INTERVAL_MILLIS = '300'
      const connectionPool = new ConnectionPool()
      pool = connectionPool.pool
      await pool.connect()
    })

    afterAll(async () => {
      if (pool) {
        pool.close()
      }
    })

    it('should initialise based on environment variables', async () => {
      let connection
      try {
        connection = await pool.acquire()
        expect(connection.config.options.port).toBe(parseInt(process.env.SQLDB_PORT))
        expect(connection.config.options.abortTransactionOnError).toBe(true)
        expect(connection.config.options.enableNumericRoundabort).toBe(false)
        expect(connection.config.options.connectTimeout).toBe(parseInt(process.env.SQLDB_CONNECTION_TIMEOUT_MILLIS))
        expect(connection.config.options.requestTimeout).toBe(parseInt(process.env.SQLDB_REQUEST_TIMEOUT_MILLIS))
        expect(connection.config.options.maxRetriesOnTransientErrors).toBe(parseInt(process.env.SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS))
        expect(connection.config.options.packetSize).toBe(parseInt(process.env.SQLDB_PACKET_SIZE))
        expect(pool.pool.acquireTimeoutMillis).toBe(parseInt(process.env.SQLDB_ACQUIRE_TIMEOUT_MILLIS))
        expect(pool.pool.createTimeoutMillis).toBe(parseInt(process.env.SQLDB_CREATE_TIMEOUT_MILLIS))
        expect(pool.pool.destroyTimeoutMillis).toBe(parseInt(process.env.SQLDB_DESTROY_TIMEOUT_MILLIS))
        expect(pool.pool.idleTimeoutMillis).toBe(parseInt(process.env.SQLDB_IDLE_TIMEOUT_MILLIS))
        expect(pool.pool.reapIntervalMillis).toBe(parseInt(process.env.SQLDB_REAP_INTERVAL_MILLIS))
        expect(pool.pool.createRetryIntervalMillis).toBe(parseInt(process.env.SQLDB_CREATE_RETRY_INTERVAL_MILLIS))
      } finally {
        if (connection) {
          await pool.release(connection)
        }
      }
    })

    it('closure should prevent furter requests from being processed', async () => {
      const expectedErrorDetails = new Error('Connection pool is closed - please restart the function app')
      const context = new Context()
      await transactionHelper.closeConnectionPool()
      await expect(transactionHelper.doInTransaction(() => {}, context)).rejects.toThrow(expectedErrorDetails)
    })
  })
})
