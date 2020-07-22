module.exports = describe('Tests for import timeseries display groups', () => {
  const dateFormat = 'YYYY-MM-DD HH:mm:ss'
  const importFromFewsMessages = require('./messages/coastal-display-group-messages')
  const { checkImportedData } = require('./display-group-test-utils')
  const Context = require('../mocks/defaultContext')
  const ConnectionPool = require('../../../Shared/connection-pool')
  const CommonCoastalTimeseriesTestUtils = require('../shared/common-coastal-timeseries-test-utils')
  const ImportFromFewsTestUtils = require('./import-from-fews-test-utils')
  const moment = require('moment')
  const sql = require('mssql')

  let context
  let importFromFewsTestUtils
  jest.mock('axios')

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonCoastalTimeseriesTestUtils = new CommonCoastalTimeseriesTestUtils(pool, importFromFewsMessages)

  describe('Message processing for coastal display group task run completion', () => {
    beforeAll(async () => {
      await commonCoastalTimeseriesTestUtils.beforeAll(pool)
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.importFromFews = []
      importFromFewsTestUtils = new ImportFromFewsTestUtils(context, pool, importFromFewsMessages, checkImportedData)
      await commonCoastalTimeseriesTestUtils.beforeEach(pool)
      await insertTimeseriesHeaders(pool)
    })
    afterAll(async () => {
      await commonCoastalTimeseriesTestUtils.afterAll(pool)
    })
    it('should import data for a single plot associated with an approved forecast task run', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      const config = {
        messageKey: 'singlePlotApprovedForecast',
        mockResponses: [mockResponse]
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should import data for multiple plots associated with an approved forecast task run', async () => {
      const mockResponses = [{
        data: {
          key: 'First plot timeseries display groups data'
        }
      },
      {
        data: {
          key: 'Second plot timeseries display groups data'
        }
      }]
      const config = {
        messageKey: 'multiplePlotApprovedForecast',
        mockResponses: mockResponses
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should not import data for an ignored forecast task run', async () => {
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('ignoredWorkflowPlot')
    })
    it('should not import data for an unapproved forecast task run', async () => {
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('unapprovedWorkflowPlot')
    })
    it('should not import data for an approved out-of-date forecast task run', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      const config = {
        messageKey: 'laterSinglePlotApprovedForecast',
        mockResponses: [mockResponse]
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('earlierSinglePlotApprovedForecast')
    })
    it('should allow the default forecast start-time and end-time to be overridden using environment variables', async () => {
      const originalEnvironment = process.env
      try {
        process.env['FEWS_START_TIME_OFFSET_HOURS'] = 24
        process.env['FEWS_END_TIME_OFFSET_HOURS'] = 48
        const mockResponse = {
          data: {
            key: 'Timeseries display groups data'
          }
        }
        const config = {
          messageKey: 'singlePlotApprovedForecast',
          mockResponses: [mockResponse]
        }
        await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
      } finally {
        process.env = originalEnvironment
      }
    })
    it('should create a staging exception when a timeseries header does not exist for a task run', async () => {
      const messageKey = 'unknownTaskRun'
      const taskRunId = importFromFewsMessages[messageKey][0].taskRunId
      await importFromFewsTestUtils.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported(messageKey, `Unable to retrieve TIMESERIES_HEADER record for task run ${taskRunId}`)
    })
    it('should create a staging exception when a message contains a plot and a filter ', async () => {
      const messageKey = 'invalidPlotAndFilterMessage'
      await importFromFewsTestUtils.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported(messageKey, `Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes`)
    })
    it('should create a staging exception when a message does not contain a task run ID ', async () => {
      const messageKey = 'missingTaskRunIdMessage'
      await importFromFewsTestUtils.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported(messageKey, `Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes`)
    })
    it('should create a staging exception when a message does not contain a plot or filter ID', async () => {
      const messageKey = 'missingTaskRunIdMessage'
      await importFromFewsTestUtils.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported(messageKey, `Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes`)
    })
    it('should create a timeseries staging exception when a message contains an unknown plot or filter ID', async () => {
      const messageKey = 'unknownPlotId'
      const expectedErrorDetails = {
        sourceId: importFromFewsMessages[messageKey][0].plotId,
        sourceType: 'P',
        csvError: true,
        csvType: 'U',
        description: `Unable to find locations for plot ${importFromFewsMessages[messageKey][0].plotId} of task run undefined in any display group CSV`
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(messageKey, null, expectedErrorDetails)
    })
    it('should throw an exception when the core engine PI server is unavailable', async () => {
      // If the core engine PI server is down messages are eligible for replay a certain number of times so check that
      // an exception is thrown to facilitate this process.
      const mockResponse = new Error('connect ECONNREFUSED mockhost')
      await importFromFewsTestUtils.processMessagesAndCheckExceptionIsThrown('singlePlotApprovedForecast', mockResponse)
    })
    it('should create a timeseries staging exception when a core engine PI server resource is unavailable', async () => {
      const mockResponse = new Error('Request failed with status code 404')
      mockResponse.response = {
        data: 'Error text',
        status: 404
      }

      const messageKey = 'singlePlotApprovedForecast'
      const expectedErrorDetails = {
        sourceId: importFromFewsMessages[messageKey][0].plotId,
        sourceType: 'P',
        csvError: false,
        csvType: null,
        payload: importFromFewsMessages[messageKey][0],
        description: `An error occured while processing data for plot ${importFromFewsMessages[messageKey][0].plotId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} (workflow Test_Coastal_Workflow): Request failed with status code 404 (${mockResponse.response.data})`
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(messageKey, [mockResponse], expectedErrorDetails)
    })
    it('should import data for a single plot associated with an approved forecast with an output binding set to active', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }

      process.env.IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED = true // in this case the build script would contain function.json with an output binding
      context.bindingDefinitions = [{ direction: 'out', name: 'stagedTimeseries', type: 'servieBus' }]

      const config = {
        messageKey: 'singlePlotApprovedForecast',
        mockResponses: [mockResponse]
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should load a single plot associated with a workflow that is also associated with non display group data', async () => {
      const request = new sql.Request(pool)
      const mockResponse = [{
        data: {
          key: 'Timeseries data'
        }
      }]

      await request.batch(`
        insert into
          fff_staging.non_display_group_workflow (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Span_Workflow', 'SpanFilter', 1, 0, 0, 'external_historical')
      `)

      const config = {
        messageKey: 'singlePlotAndFilterApprovedForecast',
        mockResponses: mockResponse
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should throw an exception when the coastal_display_group_workflow table locks due to refresh', async () => {
      // If the coastal_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await importFromFewsTestUtils.lockWorkflowTableAndCheckMessagesCannotBeProcessed('coastalDisplayGroupWorkflow', 'singlePlotApprovedForecast', mockResponse)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
  })

  async function insertTimeseriesHeaders (pool) {
    const request = new sql.Request(pool)
    const earlierTaskRunStartTime = moment.utc(importFromFewsMessages.commonMessageData.startTime).subtract(30, 'seconds')
    const earlierTaskRunCompletionTime = moment.utc(importFromFewsMessages.commonMessageData.completionTime).subtract(30, 'seconds')
    await request.input('taskRunStartTime', sql.DateTime2, importFromFewsMessages.commonMessageData.startTime)
    await request.input('taskRunCompletionTime', sql.DateTime2, importFromFewsMessages.commonMessageData.completionTime)
    await request.input('earlierTaskRunStartTime', sql.DateTime2, earlierTaskRunStartTime.format(dateFormat))
    await request.input('earlierTaskRunCompletionTime', sql.DateTime2, earlierTaskRunCompletionTime.format(dateFormat))

    await request.batch(`
      insert into
        fff_staging.timeseries_header
          (task_start_time, task_completion_time, task_run_id, workflow_id, forecast, approved, message)
      values
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000001', 'Test_Coastal_Workflow', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000002', 'Test_Coastal_Workflow2', 1, 1, '{"input": "Test message"}'),
         (@earlierTaskRunStartTime, @earlierTaskRunCompletionTime, 'ukeafffsmc00:000000003', 'Test_Coastal_Workflow1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000004', 'Test_Coastal_Workflow1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000005', 'Test_Ignored_Workflow_1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000006', 'Test_Ignored_Workflow_1', 1, 0, '{"input": "Test message"}')
    `)
  }
})
