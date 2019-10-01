const { pool, pooledConnect } = require('../Shared/connection-pool')

if (process.env['TEST_TIMEOUT']) {
  jest.setTimeout(parseInt(process.env['TEST_TIMEOUT']))
}

describe('Run all unit tests in sequence', () => {
  beforeAll(() => {
    // Ensure the connection pool is ready
    return pooledConnect
  })

  afterAll(() => {
    return pool.close()
  })

  require('../RefreshLocationLookupData/test.index')
  require('../ImportTimeSeriesDisplayGroups/test.index')
})
