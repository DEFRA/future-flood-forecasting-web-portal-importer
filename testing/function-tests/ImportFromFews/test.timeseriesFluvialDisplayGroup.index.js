module.exports = describe('Tests for import fluvial timeseries display groups', () => {
  const dateFormat = 'YYYY-MM-DD HH:mm:ss'
  const importFromFewsMessages = require('./messages/fluvial-display-group-messages')
  const { checkImportedData } = require('./display-group-test-utils')
  const Context = require('../mocks/defaultContext')
  const ConnectionPool = require('../../../Shared/connection-pool')
  const CommonFluvialTimeseriesTestUtils = require('../shared/common-fluvial-timeseries-test-utils')
  const ImportFromFewsTestUtils = require('./import-from-fews-test-utils')
  const moment = require('moment')
  const sql = require('mssql')

  let context
  let importFromFewsTestUtils
  jest.mock('axios')

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonFluvialTimeseriesTestUtils = new CommonFluvialTimeseriesTestUtils(pool, importFromFewsMessages)

  describe('Message processing for fluvial display group timeseries import ', () => {
    beforeAll(async () => {
      const request = new sql.Request(pool)
      await commonFluvialTimeseriesTestUtils.beforeAll(pool)
      await request.batch(`
        insert into
          fff_staging.non_display_group_workflow (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Span_Workflow2', 'SpanFilter2', 1, 0, 0, 'external_historical')
      `)
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.importFromFews = []
      importFromFewsTestUtils = new ImportFromFewsTestUtils(context, pool, importFromFewsMessages, checkImportedData)
      await commonFluvialTimeseriesTestUtils.beforeEach(pool)
      await insertTimeseriesHeaders(pool)
    })
    afterAll(async () => {
      await commonFluvialTimeseriesTestUtils.afterAll(pool)
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
    it('should not import data for an approved out-of-date forecast task run', async () => {
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
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('earlierSinglePlotApprovedForecast')
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
        description: `An error occured while processing data for plot ${importFromFewsMessages[messageKey][0].plotId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} (workflow Test_Fluvial_Workflow1): Request failed with status code 404 (${mockResponse.response.data})`
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(messageKey, [mockResponse], expectedErrorDetails)
    })
    it('should load a single plot associated with a workflow that is also associated with non display group data', async () => {
      const mockResponses = [
        {
          data: {
            key: 'Timeseries data'
          }
        },
        {
          data: {
            key: 'Timeseries data'
          }
        }
      ]

      const config = {
        messageKey: 'singlePlotAndFilterApprovedForecast',
        mockResponses: mockResponses
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should throw an exception when the fluvial_display_group_workflow table locks due to refresh', async () => {
      // If the fluvial_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await importFromFewsTestUtils.lockWorkflowTableAndCheckMessagesCannotBeProcessed('fluvialDisplayGroupWorkflow', 'singlePlotApprovedForecast', mockResponse)
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
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000001', 'Test_Fluvial_Workflow1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000002', 'Test_Fluvial_Workflow2', 1, 1, '{"input": "Test message"}'),
         (@earlierTaskRunStartTime, @earlierTaskRunCompletionTime, 'ukeafffsmc00:000000003', 'Test_Fluvial_Workflow1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000004', 'Span_Workflow2', 1, 1, '{"input": "Test message"}')
    `)
  }
})
