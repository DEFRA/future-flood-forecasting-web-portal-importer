module.exports = describe('Tests for preventing ignored workflow import', () => {
  const importFromFewsMessages = require('./messages/ignored-workflow-messages')
  const { checkImportedData } = require('./display-group-test-utils')
  const Context = require('../mocks/defaultContext')
  const ConnectionPool = require('../../../Shared/connection-pool')
  const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')
  const ImportFromFewsTestUtils = require('./import-from-fews-test-utils')
  const sql = require('mssql')

  let context
  let importFromFewsTestUtils
  jest.mock('axios')

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  let commonTimeseriesTestUtils

  describe('Message processing for ignored workflow timeseries import ', () => {
    beforeAll(async () => {
      await pool.connect()
      commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool, importFromFewsMessages)
      await commonTimeseriesTestUtils.beforeAll(pool)
      await insertTimeseriesHeaders(pool)
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.importFromFews = []
      importFromFewsTestUtils = new ImportFromFewsTestUtils(context, pool, importFromFewsMessages, checkImportedData)
      await commonTimeseriesTestUtils.beforeEach(pool)
    })
    afterAll(async () => {
      await commonTimeseriesTestUtils.afterAll(pool)
    })
    it('should not import data for an ignored forecast task run', async () => {
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('ignoredWorkflowPlot')
    })
  })
  async function insertTimeseriesHeaders (pool) {
    const request = new sql.Request(pool)
    await request.input('taskRunStartTime', sql.DateTime2, importFromFewsMessages.commonMessageData.startTime)
    await request.input('taskRunCompletionTime', sql.DateTime2, importFromFewsMessages.commonMessageData.completionTime)

    await request.batch(`
      insert into
        fff_staging.timeseries_header
          (task_start_time, task_completion_time, task_run_id, workflow_id, forecast, approved, message)
      values
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000001', 'Test_Ignored_Workflow_1', 1, 1, '{"input": "Test message"}')
    `)
  }
})
