const taskRunCompleteMessages = require('./messages/task-run-complete/fluvial-display-group-messages')
const CommonFluvialTimeseriesTestUtils = require('../shared/common-fluvial-timeseries-test-utils')
const ProcessFewsEventCodeTestUtils = require('./process-fews-event-code-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const sql = require('mssql')

module.exports = describe('Tests for import timeseries display groups', () => {
  let context
  let processFewsEventCodeTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonFluvialTimeseriesTestUtils = new CommonFluvialTimeseriesTestUtils(pool, taskRunCompleteMessages)

  const expectedData = {
    singlePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Fluvial Plot1']
    },
    earlierSinglePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Fluvial Plot1']
    },
    multiplePlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Fluvial Plot2a', 'Test Fluvial Plot2b']
    },
    forecastApprovedManually: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Fluvial Plot1']
    },
    approvedPartialTaskRunForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test_Partial_Taskrun_Plot']
    }
  }

  describe('Message processing for fluvial display group task run completion', () => {
    beforeAll(async () => {
      await commonFluvialTimeseriesTestUtils.beforeAll(pool)
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.importFromFews = []
      processFewsEventCodeTestUtils = new ProcessFewsEventCodeTestUtils(context, pool, taskRunCompleteMessages)
      await commonFluvialTimeseriesTestUtils.beforeEach(pool)
    })
    afterAll(async () => {
      await commonFluvialTimeseriesTestUtils.afterAll(pool)
    })
    it('should import data for a single plot associated with an approved forecast task run', async () => {
      const messageKey = 'singlePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should import data for a single plot associated with an approved forecast task run when the task run message is received as a string ', async () => {
      const messageKey = 'singlePlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey], true)
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
      context.bindings.importFromFews = [] // reset the context bindings as it stays in test memory
      const expectedNumberOfHeaderRecords = 1
      const expectedNumberOfNewOutgoingMessages = 0
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('earlierSinglePlotApprovedForecast', expectedNumberOfHeaderRecords, expectedNumberOfNewOutgoingMessages)
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
    it('should throw an exception when the core engine PI server is unavailable', async () => {
      // If the core engine PI server is down messages are eligible for replay a certain number of times so check that
      // an exception is thrown to facilitate this process.
      const mockResponse = new Error('connect ECONNREFUSED mockhost')
      await processFewsEventCodeTestUtils.processMessageAndCheckExceptionIsThrown('singlePlotApprovedForecast', mockResponse)
    })
    it('should throw an exception when a core engine PI server resource is unavailable', async () => {
      const mockResponse = new Error('Request failed with status code 404')
      mockResponse.response = {
        data: 'Error text',
        status: 404
      }
      await processFewsEventCodeTestUtils.processMessageAndCheckExceptionIsThrown('singlePlotApprovedForecast', mockResponse)
    })
    it('should import data (with no staging exceptions present) for an approved forecast task run following the earlier rejection of numerous unapproved partial taskrun messages for the same task run', async () => {
      // in the core engine a forecast taskrun can run multiple times in what are called 'partial taskruns'
      // only one partial taskrun is approved and this should come after all the preceding unapproved partial taskruns
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('unapprovedPartialTaskRunForecast')
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('unapprovedPartialTaskRunForecast')
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated('approvedPartialTaskRunForecast', expectedData['approvedPartialTaskRunForecast'])
    })
    it('should dismiss (for the same taskRun) messages for an approved forecast run and unapproved forecast run following the earlier load of the approved partial taskrun message', async () => {
      // this test simulates a situation where an unapproved partial taskrun message is received after the approved partial taskrun message

      // a taskrun partial linked to an unapproved forecast should be rejected
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('unapprovedPartialTaskRunForecast')

      // simulate a successful load for the taskRun partial linked to the approved forecast
      await insertTimeseriesHeaderAndTimeseries(pool)
      const expectedNumberOfHeaderRecords = 1 // now pre-existing for taskrun
      const expectedNumberOfNewOutgoingMessages = 0
      const expectedNumberOfStagingExceptions = 0

      // another unapproved taskrun partial linked to the forecast should be rejected (no longer the expected message sequence from core)
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('unapprovedPartialTaskRunForecast', expectedNumberOfHeaderRecords, expectedNumberOfNewOutgoingMessages, expectedNumberOfStagingExceptions)

      // another approved taskrun partial linked to the forecast should not create any new outgoing messages or staging exceptions
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('approvedPartialTaskRunForecast', expectedNumberOfHeaderRecords, expectedNumberOfNewOutgoingMessages, expectedNumberOfStagingExceptions)
    })
  })

  async function insertTimeseriesHeaderAndTimeseries (pool) {
    const request = new sql.Request(pool)
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      declare @id2 uniqueidentifier
      set @id2 = newid()
      insert into fff_staging.timeseries_header
        (id, task_completion_time, task_run_id, workflow_id, message)
      values
        (@id1, getutcdate(),'ukeafffsmc00:000000009','Test_Partial_Taskrun_Workflow', '{"key": "value"}')
      insert into fff_staging.timeseries
        (id, fews_data, fews_parameters, timeseries_header_id, import_time)
      values
        (@id2, compress('fews_data'), '&plotId=Test_Partial_Taskrun_Plot&locationIds=Test_Partial_Taskrun_Location;&startTime=more data', @id1, getutcdate())
    `
    query.replace(/"/g, "'")
    await request.query(query)
  }
})
