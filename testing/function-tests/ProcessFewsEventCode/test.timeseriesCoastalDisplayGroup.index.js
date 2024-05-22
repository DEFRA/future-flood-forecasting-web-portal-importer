const taskRunCompleteMessages = require('./messages/task-run-complete/coastal-display-group-messages')
const CommonCoastalTimeseriesTestUtils = require('../shared/common-coastal-timeseries-test-utils')
const ProcessFewsEventCodeTestUtils = require('./process-fews-event-code-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const sql = require('mssql')

jest.mock('@azure/service-bus')

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
    singlePlotAndFilterApprovedForecastWithScheduledOutputMessaging: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['SpanPlot'],
      outgoingFilterIds: ['SpanFilter'],
      scheduledMessages: true
    },
    taskRunWithStagingException: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Coastal Plot 2a', 'Test Coastal Plot 2b']
    },
    taskRunWithTimeseriesStagingExceptions: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Coastal Plot 5a', 'Test Coastal Plot 5b', 'Test Coastal Plot 5c'],
      remainingTimeseriesStagingExceptions: [{
        sourceId: 'Test Coastal Plot 5a',
        sourceType: 'P'
      }]
    },
    taskRunWithUnresolvedTimeseriesStagingExceptions: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test Coastal Plot 5b', 'Test Coastal Plot 5c'],
      remainingTimeseriesStagingExceptions: [{
        sourceId: 'Test Coastal Plot 5a',
        sourceType: 'P'
      }]
    },
    approvedPartialTaskRunSpan: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Test_Partial_Taskrun_Span_Plot'],
      outgoingFilterIds: ['Span_Filter']
    },
    idleWorkflowForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['Idle Test Workflow Plot']
    }
  }

  describe('Message processing for coastal display group task run completion', () => {
    beforeAll(async () => {
      await commonCoastalTimeseriesTestUtils.beforeAll()
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.importFromFews = []
      processFewsEventCodeTestUtils = new ProcessFewsEventCodeTestUtils(context, pool, taskRunCompleteMessages)
      await commonCoastalTimeseriesTestUtils.beforeEach()
      const request = new sql.Request(pool)
      await request.batch('delete from fff_staging.non_display_group_workflow')
    })
    afterAll(async () => {
      await commonCoastalTimeseriesTestUtils.afterAll()
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
      context.bindings.importFromFews = [] // reset the context bindings as it stays in test memory
      const expectedNumberOfHeaderRecords = 1
      const expectedNumberOfNewOutgoingMessages = 0
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('earlierSinglePlotApprovedForecast', expectedNumberOfHeaderRecords, expectedNumberOfNewOutgoingMessages)
    })
    it('should create a timeseries header and create a message for a single plot associated with a forecast task run approved manually', async () => {
      const messageKey = 'forecastApprovedManually'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should create a staging exception for an unknown forecast approved workflow and allow message replay following correction. The staging exception should be deactivated', async () => {
      const taskRunWithStagingExceptionMessageKey = 'taskRunWithStagingException'
      const unknownWorkflowMessageKey = 'unknownWorkflow'
      const workflowId = taskRunCompleteMessages[unknownWorkflowMessageKey].input.description.split(/\s+/)[1]
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated(unknownWorkflowMessageKey, `Missing PI Server input data for ${workflowId}`)
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(taskRunWithStagingExceptionMessageKey, expectedData[taskRunWithStagingExceptionMessageKey])
    })
    it('should prevent replay of a task run when all plots/filers have been processed', async () => {
      const messageKey = 'singlePlotApprovedForecast'
      await insertTimeseriesHeaderAndTimeseries(pool)
      const expectedNumberOfHeaderRecords = 1
      const expectedNumberOfNewOutgoingMessages = 0
      const expectedNumberOfStagingExceptions = 0
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated(messageKey, expectedNumberOfHeaderRecords, expectedNumberOfNewOutgoingMessages, expectedNumberOfStagingExceptions)
    })
    it('should prevent replay of a task run plot following NO resolution of invalid configuration. The timeseries staging exception should still be active', async () => {
      const messageKey = 'taskRunWithTimeseriesStagingExceptions'
      await insertTimeseriesHeaderAndTimeseriesStagingExceptions(pool, 1) // timeseries staging exception inserted after workflow refresh date
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData.taskRunWithUnresolvedTimeseriesStagingExceptions)
    })
    it('should allow replay of a task run following invalid resolution of a partial load failure. Invalid resolution is caused by a workflow plot being defined in multiple display group CSV files. The original timeseries staging exception should be deactivated', async () => {
      const messageKey = 'multiplePlotApprovedForecast'
      const request = new sql.Request(pool)
      await request.batch(`
        insert into
          fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
        values
          ('Test_Coastal_Workflow2', 'Test Coastal Plot 2a', 'Test Coastal Location 2a-1')
      `)
      await insertTimeseriesHeaderTimeseriesAndTimeseriesStagingException(pool)
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should allow replay of a task run following resolution of a partial load failure caused by a workflow plot being defined in multiple display group CSV files. The timeseries staging exception should NOT be deactivated until the resolution is processed by the ImportFromFews function', async () => {
      const messageKey = 'taskRunWithTimeseriesStagingExceptions'
      await insertTimeseriesHeaderAndTimeseriesStagingExceptionForUnknownCsv(pool, -1)
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should allow replay of a task run following resolution of a partial load failure due to invalid configuration, resulting in no timeseries data being loaded for a plot. The associated timeseries staging exception should be deactivated. Other timeseries staging exceptions should remain active', async () => {
      const messageKey = 'taskRunWithTimeseriesStagingExceptions'
      await insertTimeseriesHeaderAndTimeseriesStagingExceptions(pool, -1) // timeseries staging exception inserted before workflow refresh date
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should allow replay of a task run following resolution of a partial load failure due to invalid configuration, resulting in timeseries data being loaded for a subset of plot locations. The timeseries staging exception should be deactivated', async () => {
      const messageKey = 'multiplePlotApprovedForecast'
      await insertTimeseriesHeaderTimeseriesAndTimeseriesStagingException(pool)
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
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
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should throw an exception when the coastal display group workflow table locks due to refresh', async () => {
      // If the coastal_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      await processFewsEventCodeTestUtils.lockWorkflowTableAndCheckMessageCannotBeProcessed('coastalDisplayGroupWorkflow', 'singlePlotApprovedForecast')
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 5000)
    it('should not import data (with no staging exceptions present) for an unapproved forecast spanning task run until approved', async () => {
      const request = new sql.Request(pool)
      await request.batch(`
        insert into
          fff_staging.non_display_group_workflow (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Test_Partial_Taskrun_Span_Workflow', 'Span_Filter', 1, 0, 0, 'external_historical')
      `)
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('unapprovedPartialTaskRunSpan')
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated('approvedPartialTaskRunSpan', expectedData.approvedPartialTaskRunSpan)
    })
    it('should allow replay of a task run following resolution of a partial load failure due to invalid configuration of some location names, resulting in timeseries data being loaded for a subset of plot locations. The timeseries staging exception should NOT be deactivated by the ProcessEventCode function', async () => {
      const messageKey = 'multiplePlotApprovedForecast'
      await insertTimeseriesHeaderTimeseriesAndTimeseriesStagingExceptionPartialBadLocation(pool)
      const updatedExpectedData = expectedData[messageKey]
      // The plot is not misspelled so ProcessEventCode will not remove this timeseries staging exception
      updatedExpectedData.remainingTimeseriesStagingExceptions = [{
        sourceId: 'Test Coastal Plot 2a',
        sourceType: 'P'
      }]
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, updatedExpectedData)
    })
    it('should create a timeseries header and create a message for a single plot associated with an approved forecast task run of a workflow with an identifer beginning with the characters id (case insensitive)', async () => {
      const messageKey = 'idleWorkflowForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should create a timeseries header and create scheduled messages for a workflow task run associated with a single plot and a single filter when the maximum amount of time to allow for PI Server indexing has not been reached', async () => {
      const request = new sql.Request(pool)
      await request.batch(`
        insert into
          fff_staging.non_display_group_workflow (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Span_Workflow', 'SpanFilter', 1, 0, 0, 'external_historical')
      `)
      const messageKey = 'singlePlotAndFilterApprovedForecastWithScheduledOutputMessaging'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
  })

  async function insertTimeseriesHeaderAndTimeseries (pool) {
    const request = new sql.Request(pool)
    const message = JSON.stringify(taskRunCompleteMessages.singlePlotApprovedForecast)
    const taskRunStartTime = taskRunCompleteMessages.commonMessageData.startTime
    const taskRunCompletionTime = taskRunCompleteMessages.commonMessageData.completionTime
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      declare @id2 uniqueidentifier
      set @id2 = newid()
      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id1, convert(datetime2, '${taskRunStartTime}', 126) at time zone 'utc', convert(datetime2, '${taskRunCompletionTime}', 126) at time zone 'utc', 1, 1, 'ukeafffsmc00:000000001','Test_Coastal_Workflow', '${message}')
      insert into fff_staging.timeseries
        (id, fews_data, fews_parameters, timeseries_header_id, import_time)
      values
        (@id2, compress('fews_data'), '&plotId=Test Coastal Plot&locationIds=Test Coastal Location&startTime=more data', @id1, getutcdate())
    `
    query.replace(/"/g, "'")
    await request.query(query)
  }

  async function insertTimeseriesHeaderTimeseriesAndTimeseriesStagingException (pool) {
    const request = new sql.Request(pool)
    const message = JSON.stringify(taskRunCompleteMessages.multiplePlotApprovedForecast)
    const taskRunStartTime = taskRunCompleteMessages.commonMessageData.startTime
    const taskRunCompletionTime = taskRunCompleteMessages.commonMessageData.completionTime
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      declare @id2 uniqueidentifier
      set @id2 = newid()
      declare @id3 uniqueidentifier
      set @id3 = newid()
      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id1, convert(datetime2, '${taskRunStartTime}', 126) at time zone 'utc', convert(datetime2, '${taskRunCompletionTime}', 126) at time zone 'utc', 1, 1, 'ukeafffsmc00:000000002','Test_Coastal_Workflow2', '${message}')
      insert into fff_staging.timeseries
        (id, fews_data, fews_parameters, timeseries_header_id, import_time)
      values
        (@id2, compress('fews_data'), '&plotId=Test Coastal Plot 2a&locationIds=Test Coastal Location 2a-1;&startTime=more data', @id1, getutcdate())
      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id3, 'Test Coastal Plot 2a typo', 'P', 1, 'C', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000002", "plotId": "Test Coastal Plot 2a typo"}', @id1, 'Error text', dateadd(hour, -1, getutcdate()))
    `
    query.replace(/"/g, "'")
    await request.query(query)
  }

  async function insertTimeseriesHeaderTimeseriesAndTimeseriesStagingExceptionPartialBadLocation (pool) {
    const request = new sql.Request(pool)
    const message = JSON.stringify(taskRunCompleteMessages.multiplePlotApprovedForecast)
    const taskRunStartTime = taskRunCompleteMessages.commonMessageData.startTime
    const taskRunCompletionTime = taskRunCompleteMessages.commonMessageData.completionTime
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      declare @id2 uniqueidentifier
      set @id2 = newid()
      declare @id3 uniqueidentifier
      set @id3 = newid()
      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id1, convert(datetime2, '${taskRunStartTime}', 126) at time zone 'utc', convert(datetime2, '${taskRunCompletionTime}', 126) at time zone 'utc', 1, 1, 'ukeafffsmc00:000000002','Test_Coastal_Workflow2', '${message}')
      insert into fff_staging.timeseries
        (id, fews_data, fews_parameters, timeseries_header_id, import_time)
      values
        (@id2, compress('fews_data'), '&plotId=Test Coastal Plot 2a&locationIds=Test Coastal Location 2a-1;&startTime=more data', @id1, getutcdate())
      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id3, 'Test Coastal Plot 2a', 'P', 1, 'C', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000002", "plotId": "Test Coastal Plot 2a"}', @id1, 'Error text', dateadd(hour, -1, getutcdate()))
    `
    query.replace(/"/g, "'")
    await request.query(query)
  }

  async function insertTimeseriesHeaderAndTimeseriesStagingExceptionForUnknownCsv (pool, exceptionTimeOffset) {
    const exceptionTime = `dateadd(hour, ${exceptionTimeOffset}, getutcdate())`
    const request = new sql.Request(pool)
    const message = JSON.stringify(taskRunCompleteMessages.taskRunWithTimeseriesStagingExceptions)
    const taskRunStartTime = taskRunCompleteMessages.commonMessageData.startTime
    const taskRunCompletionTime = taskRunCompleteMessages.commonMessageData.completionTime
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      declare @id2 uniqueidentifier
      set @id2 = newid()
      declare @id3 uniqueidentifier
      set @id3 = newid()
      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id1, convert(datetime2, '${taskRunStartTime}', 126) at time zone 'utc', convert(datetime2, '${taskRunCompletionTime}', 126) at time zone 'utc', 1, 1, 'ukeafffsmc00:000000003', 'Test_Coastal_Workflow5', '${message}')
      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id3, 'Test Coastal Plot 5a', 'P', 1, 'U', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "plotId": "Test Coastal Plot 5a"}', @id1, 'Error text', ${exceptionTime})
    `
    query.replace(/"/g, "'")
    await request.query(query)
  }

  async function insertTimeseriesHeaderAndTimeseriesStagingExceptions (pool, exceptionTimeOffset) {
    // the workflow (reference data) refresh table updates at the start of this test file
    const exceptionTime = `dateadd(hour, ${exceptionTimeOffset}, getutcdate())`
    const request = new sql.Request(pool)
    const message = JSON.stringify(taskRunCompleteMessages.taskRunWithTimeseriesStagingExceptions)
    const taskRunStartTime = taskRunCompleteMessages.commonMessageData.startTime
    const taskRunCompletionTime = taskRunCompleteMessages.commonMessageData.completionTime
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      declare @id2 uniqueidentifier
      set @id2 = newid()
      declare @id3 uniqueidentifier
      set @id3 = newid()
      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id1, convert(datetime2, '${taskRunStartTime}', 126) at time zone 'utc', convert(datetime2, '${taskRunCompletionTime}', 126) at time zone 'utc', 1, 1, 'ukeafffsmc00:000000003', 'Test_Coastal_Workflow5', '${message}')
      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id2, 'error_plot', 'P', 1, 'C', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "plotId": "error_plot"}', @id1, 'Error text', ${exceptionTime})
      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id3, 'Test Coastal Plot 5a', 'P', 1, 'C', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "plotId": "Test Coastal Plot 5a"}', @id1, 'Error text', ${exceptionTime})
    `
    query.replace(/"/g, "'")
    await request.query(query)
  }
})
