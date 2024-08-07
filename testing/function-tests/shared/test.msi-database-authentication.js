import { beforeAll, describe, expect, it, vi } from 'vitest'

export const msiDatabaseAuthenticationTests = () => describe('Test configuration for MSI database authentication', () => {
  describe('MSI database authentication', () => {
    beforeAll(() => {
      // Reset modules so that ../../../Shared/utils.js is reloaded without the
      // PI_SERVER_CALL_TIMEOUT environment variable set. This increases test coverage.
      vi.resetModules()
    })
    it('should be configured when enabled', async () => {
      let pool
      // Remove the NODE_ENV environment variable to increase test coverage.
      delete process.env.NODE_ENV
      process.env.AUTHENTICATE_WITH_MSI = true
      process.env.MSI_ENDPOINT = 'msi-endopoint-id'
      process.env.MSI_SECRET = 'msi-secret'
      try {
        const ConnectionPool = (await import('../../../Shared/connection-pool.js')).default
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
