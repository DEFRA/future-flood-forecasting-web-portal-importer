const ConnectionPool = require('../../../Shared/connection-pool')

module.exports = describe('Test configuration for MSI database authentication', () => {
  describe('MSI database authentication', () => {
    it('should be configured when enabled', async () => {
      let pool
      process.env.AUTHENTICATE_WITH_MSI = true
      process.env.MSI_ENDPOINT = 'msi-endopoint-id'
      process.env.MSI_SECRET = 'msi-secret'
      try {
        const connectionPool = new ConnectionPool()
        pool = connectionPool.pool
        // Attempting MSI based authentication in a unit test environment
        // should prevent database connectivity
        expect(pool.connected).toBe(false)
        expect(pool.config.authentication.options.msiEndpoint).toBe(process.env.MSI_ENDPOINT)
        expect(pool.config.authentication.options.msiSecret).toBe(process.env.MSI_SECRET)
      } finally {
        if (pool) {
          await pool.close()
        }
      }
    })
  })
})
