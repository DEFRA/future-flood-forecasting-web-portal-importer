const { doInTransaction } = require('../../../Shared/transaction-helper')
const getBooleanIndicator = require('../../../ProcessFewsEventCode/helpers/get-boolean-indicator')
const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const { isBoolean } = require('../../../Shared/utils')
const Context = require('../mocks/defaultContext')

const jestConnectionPool = new ConnectionPool()
const pool = jestConnectionPool.pool
const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)

let context
module.exports = describe('Test forecast flags', () => {
  describe('Forecast flag testing ', () => {
    beforeAll(async () => {
      await commonTimeseriesTestUtils.beforeAll()
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      await commonTimeseriesTestUtils.beforeEach()
    })

    afterAll(async () => {
      await commonTimeseriesTestUtils.afterAll()
    })
    it('should return undefined when a message does not contain an unexpected boolean value', async () => {
      await doInTransaction({ fn: testInTransaction, context, errorMessage: 'Error' })
    })
    it('should return true for boolean values', () => {
      expect(isBoolean(true)).toBe(true)
      expect(isBoolean(false)).toBe(true)
    })
    it('should return true for boolean string values regardless of case', () => {
      expect(isBoolean('True')).toBe(true)
      expect(isBoolean('false')).toBe(true)
    })
    it('should return false for non-boolean values', () => {
      expect(isBoolean(0)).toBe(false)
      expect(isBoolean('string')).toBe(false)
    })
  })
})

async function testInTransaction (transaction, context) {
  const taskRunData = {
    message: 'input',
    transaction
  }
  expect(await getBooleanIndicator(context, taskRunData, 'Approved')).toBe(undefined)
}
