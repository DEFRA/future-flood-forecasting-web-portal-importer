const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const replayProcessFewsEventCode = require('../../../ReplayProcessFewsEventCode')
const messages = require('../ProcessFewsEventCode/messages/task-run-complete/fluvial-display-group-messages.json')

module.exports = describe('Tests for replaying messages on the ProcessFewsEventCode dead letter queue', () => {
  let context
  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)

  describe('Message processing for the ProcessFewsEventCode dead letter queue', () => {
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

    it('should transfer messages containing valid JSON to the fews-eventcode-queue', async () => {
      await replayProcessFewsEventCode(context, messages.singlePlotApprovedForecast)
      expect(context.bindings.processFewsEventCode).toBe(messages.singlePlotApprovedForecast)
    })

    it('should transfer messages containing invalid JSON to the fews-eventcode-queue', async () => {
      // Ensure the message contains invalid JSON.
      const message = '{ "message": "A:\\\Path"}' // eslint-disable-line
      const stringifiedMessage = JSON.stringify(message)
      await replayProcessFewsEventCode(context, message)
      expect(context.bindings.processFewsEventCode).toBe(stringifiedMessage)
    })
  })
})
