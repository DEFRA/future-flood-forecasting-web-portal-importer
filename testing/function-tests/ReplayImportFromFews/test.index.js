import { loadJsonFile } from '../../../Shared/utils.js'
import CommonTimeseriesTestUtils from '../shared/common-timeseries-test-utils.js'
import ConnectionPool from '../../../Shared/connection-pool.js'
import Context from '../mocks/defaultContext.js'
import replayImportFromFews from '../../../ReplayImportFromFews/index.mjs'

export const replayDeadLetteredImportFromFewsMessageTests = () => describe('Tests for replaying dead lettered ImportFromFews messages', () => {
  let context
  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)

  const messages = loadJsonFile('testing/function-tests/ImportFromFews/messages/fluvial-display-group-messages.json')

  describe('Message processing for the ImportFromFews dead letter queue', () => {
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
