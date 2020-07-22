module.exports = describe('Tests for import timeseries display groups', () => {
  const taskRunCompleteMessages = require('./messages/task-run-complete/fluvial-display-group-messages')
  const Context = require('../mocks/defaultContext')
  const ConnectionPool = require('../../../Shared/connection-pool')
  const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')
  const ProcessFewsEventCodeTestUtils = require('./process-fews-event-code-test-utils')
  const sql = require('mssql')

  let context
  let processFewsEventCodeTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  const request = new sql.Request(pool)

  const expectedData = {
    singlePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Plot1' ]
    },
    earlierSinglePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Plot1' ]
    },
    multiplePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Plot2a', 'Test Plot2a' ]
    },
    forecastApprovedManually: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Plot1' ]
    }
  }

  describe('Message processing for fluvial display group task run completion', () => {
    beforeAll(async () => {
      await commonTimeseriesTestUtils.beforeAll(pool)
      await request.batch(`
        insert into
          fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
        values
          ('Test_Fluvial_Workflow1', 'Test Plot1', 'Test Location1'),
          ('Test_Fluvial_Workflow2', 'Test Plot2a', 'Test Location2a'),
          ('Test_Fluvial_Workflow2', 'Test Plot2b', 'Test Location2b')
      `)
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
    it('should import data for a single plot associated with an approved forecast task run', async () => {
      const messageKey = 'singlePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should import data for multiple plots associated with an approved forecast task run', async () => {
      const messageKey = 'multiplePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should not import data for an unapproved forecast task run', async () => {
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('unapprovedForecast')
    })
    it('should not import data for an out-of-date forecast approved task run', async () => {
      const messageKey = 'singlePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('earlierSinglePlotApprovedForecast', 1, 1)
    })
    it('should import data for a forecast manually approved task run', async () => {
      const messageKey = 'forecastApprovedManually'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should create a staging exception for an unknown forecast approved workflow', async () => {
      const unknownWorkflow = 'unknownWorkflow'
      const workflowId = taskRunCompleteMessages[unknownWorkflow].input.description.split(/\s+/)[1]
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated(unknownWorkflow, `Missing PI Server input data for ${workflowId}`)
    })
    it('should create a staging exception for a message missing task run approval information', async () => {
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated('forecastWithoutApprovalStatus', 'Unable to extract task run Approved status from message')
    })
    it('should throw an exception when the fluvial display group workflow table locks due to refresh', async () => {
      // If the fluvial_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      await processFewsEventCodeTestUtils.lockWorkflowTableAndCheckMessageCannotBeProcessed('fluvialDisplayGroupWorkflow', 'singlePlotApprovedForecast')
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
  })
})
