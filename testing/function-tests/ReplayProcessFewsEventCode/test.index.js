import { loadJsonFile } from '../../../Shared/utils.js'
import CommonTimeseriesTestUtils from '../shared/common-timeseries-test-utils.js'
import ConnectionPool from '../../../Shared/connection-pool.js'
import Context from '../mocks/defaultContext.js'
import replayProcessFewsEventCode from '../../../ReplayProcessFewsEventCode/index.mjs'
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'

const messages = loadJsonFile('testing/function-tests/ProcessFewsEventCode/messages/task-run-complete/fluvial-display-group-messages.json')

export const replayDeadLetteredProcessFewsEventCodeMessageTests = () => describe('Tests for replaying dead lettered ProcessFewsEventCode messages', () => {
  let context
  const viConnectionPool = new ConnectionPool()
  const pool = viConnectionPool.pool
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)

  describe('Message processing for the ProcessFewsEventCode dead letter queue', () => {
    beforeAll(async () => {
      await commonTimeseriesTestUtils.beforeAll()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Vitest mock
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
