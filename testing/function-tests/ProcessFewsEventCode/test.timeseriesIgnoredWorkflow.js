module.exports = describe('Tests for import timeseries ignored workflows', () => {
  const taskRunCompleteMessages = require('./messages/task-run-complete/ignored-workflow-messages')
  const Context = require('../mocks/defaultContext')
  const ConnectionPool = require('../../../Shared/connection-pool')
  const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')
  const ProcessFewsEventCodeTestUtils = require('./process-fews-event-code-test-utils')

  let context
  let processFewsEventCodeTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)

  jest.mock('axios')

  describe('Message processing for ignored workflows', () => {
    beforeAll(async () => {
      await commonTimeseriesTestUtils.beforeAll(pool)
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.importFromFews = []
      processFewsEventCodeTestUtils = new ProcessFewsEventCodeTestUtils(context, pool, taskRunCompleteMessages)
      await commonTimeseriesTestUtils.beforeEach(pool)
    })

    afterAll(async () => {
      await commonTimeseriesTestUtils.afterAll(pool)
    })

    it('should reject an ignored workflow', async () => {
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('ignoredForecast')
    })
    it('should throw an exception when the ignored workflow table locks due to refresh', async () => {
      // If the ignored table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      await processFewsEventCodeTestUtils.lockWorkflowTableAndCheckMessageCannotBeProcessed('ignoredWorkflow', 'ignoredForecast')
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    it('should create a staging exception for an invalid message', async () => {
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated('forecastWithoutApprovalStatus', 'Unable to extract task run Approved status from message')
    })
  })
})
