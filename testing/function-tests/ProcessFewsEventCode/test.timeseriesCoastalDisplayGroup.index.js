module.exports = describe('Tests for import timeseries display groups', () => {
  const taskRunCompleteMessages = require('./messages/task-run-complete/coastal-display-group-messages')
  const Context = require('../mocks/defaultContext')
  const ConnectionPool = require('../../../Shared/connection-pool')
  const CommonCoastalTimeseriesTestUtils = require('../shared/common-coastal-timeseries-test-utils')
  const ProcessFewsEventCodeTestUtils = require('./process-fews-event-code-test-utils')
  const sql = require('mssql')

  let context
  let processFewsEventCodeTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonCoastalTimeseriesTestUtils = new CommonCoastalTimeseriesTestUtils(pool, taskRunCompleteMessages)
  const request = new sql.Request(pool)

  const expectedData = {
    singlePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Coastal Plot' ]
    },
    earlierSinglePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Coastal Plot 1' ]
    },
    laterSinglePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Coastal Plot 1' ]
    },
    multiplePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Coastal Plot 2a', 'Test Coastal Plot 2b' ]
    },
    forecastApprovedManually: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Coastal Plot 1' ]
    },
    singlePlotAndFilterApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test_Plot' ],
      outgoingFilterIds: [ 'SpanFilter' ]
    }
  }

  describe('Message processing for coastal display group task run completion', () => {
    beforeAll(async () => {
      await commonCoastalTimeseriesTestUtils.beforeAll(pool)
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.importFromFews = []
      processFewsEventCodeTestUtils = new ProcessFewsEventCodeTestUtils(context, pool, taskRunCompleteMessages)
      await commonCoastalTimeseriesTestUtils.beforeEach(pool)
    })
    afterAll(async () => {
      await commonCoastalTimeseriesTestUtils.afterAll(pool)
    })
    it('should create a timeseries header and create a message for a single plot associated with an approved forecast task run', async () => {
      const messageKey = 'singlePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should create a timeseries header and create messages for multiple plots associated with an approved forecast task run', async () => {
      const messageKey = 'multiplePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should ignore an unapproved forecast task run', async () => {
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('unapprovedForecast')
    })
    it('should ignore an approved out of date forecast task run', async () => {
      const messageKey = 'laterSinglePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('earlierSinglePlotApprovedForecast', 1, 1)
    })
    it('should create a timeseries header and create a message for a single plot associated with a forecast task run approved manually', async () => {
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
    it('should create a timeseries header and create messages for a workflow task run associated with a single plot and a single filter', async () => {
      await request.batch(`
        insert into
          fff_staging.non_display_group_workflow (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Span_Workflow', 'SpanFilter', 1, 0, 0, 'external_historical')
      `)
      const messageKey = 'singlePlotAndFilterApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should throw an exception when the coastal display group workflow table locks due to refresh', async () => {
      // If the coastal_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      await processFewsEventCodeTestUtils.lockDisplayGroupTableAndCheckMessageCannotBeProcessed('coastalDisplayGroupWorkflow', 'singlePlotApprovedForecast')
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
  })
})
