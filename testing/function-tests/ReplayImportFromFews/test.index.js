const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const replayImportFromFews = require('../../../ReplayImportFromFews')
const messages = require('../ImportFromFews/messages/fluvial-display-group-messages.json')

module.exports = describe('Tests for replaying messages on the ImportFromFews dead letter queue', () => {
  let context
  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)

  describe('Message processing for the ImportFromFews dead letter queue', () => {
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

    it('should transfer object messages to the fews-import-queue', async () => {
      await replayImportFromFews(context, messages.singlePlotApprovedForecast[0])
      expect(context.bindings.importFromFews).toBe(messages.singlePlotApprovedForecast[0])
    })
    it('should transfer non-object messages to the fews-import-queue', async () => {
      await replayImportFromFews(context, JSON.stringify(messages.singlePlotApprovedForecast[0]))
      expect(context.bindings.importFromFews).toBe(JSON.stringify(messages.singlePlotApprovedForecast[0]))
    })
  })
})
