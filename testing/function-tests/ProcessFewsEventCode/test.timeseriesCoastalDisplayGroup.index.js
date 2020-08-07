const taskRunCompleteMessages = require('./messages/task-run-complete/coastal-display-group-messages')
const CommonCoastalTimeseriesTestUtils = require('../shared/common-coastal-timeseries-test-utils')
const ProcessFewsEventCodeTestUtils = require('./process-fews-event-code-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const sql = require('mssql')

module.exports = describe('Tests for import timeseries display groups', () => {
  let context
  let processFewsEventCodeTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonCoastalTimeseriesTestUtils = new CommonCoastalTimeseriesTestUtils(pool, taskRunCompleteMessages)

  const expectedData = {
    singlePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Coastal Plot']
    },
    earlierSinglePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Coastal Plot 1']
    },
    laterSinglePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Coastal Plot 1']
    },
    multiplePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Coastal Plot 2a', 'Test Coastal Plot 2b']
    },
    forecastApprovedManually: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Coastal Plot 1']
    },
    singlePlotAndFilterApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['SpanPlot'],
      outgoingFilterIds: ['SpanFilter']
    },
    taskRunWithStagingException: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Coastal Plot 2a', 'Test Coastal Plot 2b']
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
      await processFewsEventCodeTestUtils.processMessageCheckDataIsCreatedAndNoStagingExceptionsExist(messageKey, expectedData[messageKey])
    })
    it('should create a timeseries header and create messages for multiple plots associated with an approved forecast task run', async () => {
      const messageKey = 'multiplePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageCheckDataIsCreatedAndNoStagingExceptionsExist(messageKey, expectedData[messageKey])
    })
    it('should ignore an unapproved forecast task run', async () => {
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('unapprovedForecast')
    })
    it('should ignore an approved out of date forecast task run', async () => {
      const messageKey = 'laterSinglePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageCheckDataIsCreatedAndNoStagingExceptionsExist(messageKey, expectedData[messageKey])
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('earlierSinglePlotApprovedForecast', 1, 1)
    })
    it('should create a timeseries header and create a message for a single plot associated with a forecast task run approved manually', async () => {
      const messageKey = 'forecastApprovedManually'
      await processFewsEventCodeTestUtils.processMessageCheckDataIsCreatedAndNoStagingExceptionsExist(messageKey, expectedData[messageKey])
    })
    it('should create a staging exception for an unknown forecast approved workflow and allow message replay following correction', async () => {
      const taskRunWithStagingExceptionMessageKey = 'taskRunWithStagingException'
      const unknownWorkflowMessageKey = 'unknownWorkflow'
      const workflowId = taskRunCompleteMessages[unknownWorkflowMessageKey].input.description.split(/\s+/)[1]
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated(unknownWorkflowMessageKey, `Missing PI Server input data for ${workflowId}`)
      await processFewsEventCodeTestUtils.processMessageCheckDataIsCreatedAndNoStagingExceptionsExist(taskRunWithStagingExceptionMessageKey, expectedData[taskRunWithStagingExceptionMessageKey])
    })
    it('should prevent replay of a task run associated with a timeseries staging exception', async () => {
      const messageKey = 'workflowWithTimeseriesStagingException'
      await insertTimeseriesHeaderAndTimeseriesStagingException(pool)
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated(messageKey, 1)
    })
    it('should create a staging exception for a message missing task run approval information', async () => {
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated('forecastWithoutApprovalStatus', 'Unable to extract task run Approved status from message')
    })
    it('should create a timeseries header and create messages for a workflow task run associated with a single plot and a single filter', async () => {
      const request = new sql.Request(pool)
      await request.batch(`
        insert into
          fff_staging.non_display_group_workflow (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Span_Workflow', 'SpanFilter', 1, 0, 0, 'external_historical')
      `)
      const messageKey = 'singlePlotAndFilterApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageCheckDataIsCreatedAndNoStagingExceptionsExist(messageKey, expectedData[messageKey])
    })
    it('should throw an exception when the coastal display group workflow table locks due to refresh', async () => {
      // If the coastal_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      await processFewsEventCodeTestUtils.lockWorkflowTableAndCheckMessageCannotBeProcessed('coastalDisplayGroupWorkflow', 'singlePlotApprovedForecast')
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
  })

  async function insertTimeseriesHeaderAndTimeseriesStagingException (pool) {
    const request = new sql.Request(pool)
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      declare @id2 uniqueidentifier
      set @id2 = newid()
      insert into fff_staging.timeseries_header
        (id, task_completion_time, task_run_id, workflow_id, message)
      values
        (@id1, getutcdate(),'ukeafffsmc00:000000003','Test_Coastal_Workflow5', '{"key": "value"}')
      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description)
      values
        (@id2, 'error_plot', 'P', 1, 'C', 'error_plot_fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "plotId": "error_plot"}', @id1, 'Error plot text')
    `
    query.replace(/"/g, "'")
    await request.query(query)
  }
})
