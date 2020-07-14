module.exports = describe('Tests for import timeseries non-display groups', () => {
  const taskRunCompleteMessages = require('../testing/messages/task-run-complete/non-display-group-messages')
  const Context = require('../testing/mocks/defaultContext')
  const ConnectionPool = require('../Shared/connection-pool')
  const CommonTimeseriesTestUtils = require('../testing/shared/common-timeseries-test-utils')
  const ProcessFewsEventCodeTestUtils = require('../testing/shared/process-fews-event-code-test-utils')
  const sql = require('mssql')

  let context
  let processFewsEventCodeTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  const request = new sql.Request(pool)

  const expectedData = {
    singleFilterTaskRun: {
      forecast: true,
      approved: true,
      outgoingFilterIds: [ 'Test Filter3' ]
    },
    earlierSingleFilterTaskRun: {
      forecast: true,
      approved: true,
      outgoingFilterIds: [ 'Test Filter3' ]
    },
    singleFilterNonForecast: {
      forecast: false,
      approved: false,
      outgoingFilterIds: [ 'Test Filter1' ]
    },
    multipleFilterNonForecast: {
      forecast: false,
      approved: false,
      outgoingFilterIds: [ 'Test Filter2a', 'Test Filter2b' ]
    },
    singleFilterApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingFilterIds: [ 'Test Filter1' ]
    },
    filterAndPlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: [ 'Test Coastal Plot 1' ],
      outgoingFilterIds: [ 'SpanFilter' ]
    }
  }

  describe('Message processing for non display group task run completion', () => {
    beforeAll(async () => {
      await commonTimeseriesTestUtils.beforeAll(pool)
      await request.batch(`
        insert into
          fff_staging.non_display_group_workflow
             (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Test_Workflow1', 'Test Filter1', 0, 0, 0, 'external_historical'),
          ('Test_Workflow2', 'Test Filter2a', 0, 0, 0, 'external_historical'),
          ('Test_Workflow2', 'Test Filter2b', 0, 0, 0, 'external_historical'),
          ('Test_Workflow3', 'Test Filter3', 0, 0, 0, 'external_historical'),
          ('Test_Workflow4', 'Test Filter4', 0, 0, 0, 'external_historical'),
          ('Span_Workflow', 'Span Filter', 1, 0, 0, 'external_historical'),
          ('Test_workflowCustomTimes', 'Test FilterCustomTimes', 1, '10', '20', 'external_historical'),
          ('workflow_simulated_forecasting', 'Test Filter SF', 1, 0, 0, 'simulated_forecasting'),
          ('workflow_external_forecasting', 'Test Filter EF', 0, 0, 0, 'external_forecasting'),
          ('workflow_external_historical', 'Test Filter EH', 0, 0, 0, 'external_historical')
      `)
      await request.batch(`
        insert into
          fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
        values
          ('Test_Workflow4', 'Test Plot4', 'Test Location4'),
          ('Span_Workflow', 'Span Plot', 'Span Location' )
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

    it('should create a timeseries header and create a message for a single filter associated with a non-forecast task run', async () => {
      const messageKey = 'singleFilterNonForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should create a timeseries header and create messages for multiple filters associated with a non-forecast task run', async () => {
      const messageKey = 'multipleFilterNonForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should create a timeseries header and create a message for a single filter associated with an approved forecast task run', async () => {
      const messageKey = 'singleFilterApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should create a timeseries header and create messages for a approved forecast workflow task run associated with a single plot and a single filter', async () => {
      const messageKey = 'filterAndPlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should ignore an approved out of date forecast task run', async () => {
      const messageKey = 'singleFilterTaskRun'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('earlierSingleFilterTaskRun', 1, 1)
    })
    it('should create a staging exception for an unknown workflow', async () => {
      const unknownWorkflow = 'unknownWorkflow'
      const workflowId = taskRunCompleteMessages[unknownWorkflow].input.description.split(/\s+/)[1]
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated(unknownWorkflow, `Missing PI Server input data for ${workflowId}`)
    })
    it('should create a staging exception for a non-forecast without an approval status in the message', async () => {
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated('nonForecastWithoutApprovalStatus', 'Unable to extract task run Approved status from message')
    })
    it('should create a staging exception for a message containing the boolean false', async () => {
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated('booleanFalseMessage', 'Message must be either a string or a pure object', true)
    })
    it('should create a staging exception for a message containing the number 1', async () => {
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated('numericMessage', 'Message must be either a string or a pure object')
    })
    it('should ignore a missing message', async () => {
      let messageKey
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated(messageKey)
    })
    it('should ignore an empty message', async () => {
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('emptyMessage')
    })
    it('should create a staging exception for a non-forecast without an end time', async () => {
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated('nonForecastWithoutEndTime', 'Unable to extract task run completion date from message')
    })
    it('should throw an exception when the non-display group workflow table locks due to refresh', async () => {
      // If the non_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      await processFewsEventCodeTestUtils.lockDisplayGroupTableAndCheckMessageCannotBeProcessed('nonDisplayGroupWorkflow', 'singleFilterApprovedForecast')
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    it('should load a single filter associated with a workflow that is also associated with display group data', async () => {
      await request.batch(`
      insert into
        fff_staging.coastal_display_group_workflow (workflow_id, plot_id, location_ids)
      values
        ('Dual_Workflow', 'Test Coastal Plot 1', 'Test Coastal Location 1')
      `)
      const messageKey = 'filterAndPlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
  })
})
