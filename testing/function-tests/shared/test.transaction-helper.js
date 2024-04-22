const sql = require('mssql')
const { doInTransaction } = require('../../../Shared/transaction-helper')
const CommonTimeseriesTestUtils = require('./common-timeseries-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')

const jestConnectionPool = new ConnectionPool()
const pool = jestConnectionPool.pool
const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)

let context

module.exports = describe('Test transaction helper', () => {
  describe('The transaction helper', () => {
    beforeAll(async () => {
      await commonTimeseriesTestUtils.beforeAll(pool)
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      await commonTimeseriesTestUtils.beforeEach(pool)
    })

    afterAll(async () => {
      await commonTimeseriesTestUtils.afterAll(pool)
    })
    it('should throw an exception following a failure to commit a transaction', async () => {
      const errorMessage = 'ERROR'
      jest.spyOn(sql, 'Request').mockImplementation(() => {
        return {
          batch: jest.fn(),
          query: jest.fn()
        }
      })
      jest.spyOn(sql, 'Transaction').mockImplementation(() => {
        return {
          begin: jest.fn(),
          commit: jest.fn().mockImplementation(() => {
            throw new Error(errorMessage)
          })
        }
      })
      await expect(doInTransaction({ fn: performQuery, context, errorMessage: 'Error' })).rejects.toThrow(errorMessage)
    })
  })
})

async function performQuery (transaction, context) {
  const request = sql.Request(transaction)
  await request.query('select 1')
}
