const CommonFluvialTimeseriesTestUtils = require('../shared/common-fluvial-timeseries-test-utils')
const importFromFewsMessages = require('./messages/fluvial-display-group-messages')
const ImportFromFewsTestUtils = require('./import-from-fews-test-utils')
const { checkImportedData } = require('./display-group-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const moment = require('moment')
const sql = require('mssql')

module.exports = describe('Tests for import fluvial timeseries display groups', () => {
  let context
  let importFromFewsTestUtils

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
          ('Span_Workflow2', 'SpanFilter2', 1, 0, 0, 'external_historical'),
          ('Span_Workflow3', 'SpanFilterOffset', 1, 10, 11, 'external_historical')
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
        mockResponses
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

      // Add a staging exception to an obsolete forecast to ensure it is deactivated.
      const exceptionTime = moment.utc(importFromFewsMessages.commonMessageData.completionTime).subtract(15, 'seconds')
      const request = new sql.Request(pool)
      await request.input('exceptionTime', sql.DateTimeOffset, exceptionTime.toISOString())
      await request.query(`
        insert into
          fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
        values
          ('taskRunId invalid message', 'Error', 'ukeafffsmc00:000000003', 'I', 'Test_Fluvial_Workflow1', @exceptionTime);
      `)
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
      const config = {
        messageKey,
        mockResponses: [mockResponse],
        expectedErrorDetails: {
          sourceId: importFromFewsMessages[messageKey][0].plotId,
          sourceType: 'P',
          csvError: false,
          csvType: null,
          payload: importFromFewsMessages[messageKey][0],
          description: `An error occurred while processing data for plot ${importFromFewsMessages[messageKey][0].plotId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} (workflow Test_Fluvial_Workflow1): Request failed with status code 404 (${mockResponse.response.data})`
        }
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(config)
    })
    it('should create a timeseries staging exception when one or more locations linked to a plot do not exist and allow replay following CSV resolution of the problem', async () => {
      const messageKey = 'singlePlotApprovedForecastWithSomeKnownLocations'
      const badRequestMockResponse = new Error('Request failed with status code 400')
      badRequestMockResponse.response = {
        data: 'Location Test Location3c does not exists Location Test Location3d does not exists',
        status: 400
      }

      const knownLocationsMockReponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }

      const initialLocationData = {
        plotId: importFromFewsMessages[messageKey][0].plotId,
        includedLocations: ['Test Location3a', 'Test Location3b'],
        excludedLocations: ['Test Location3c', 'Test Location3d']
      }

      const config = [
        {
          messageKey,
          mockResponses: [badRequestMockResponse, knownLocationsMockReponse],
          expectedErrorDetails: {
            sourceId: importFromFewsMessages[messageKey][0].plotId,
            sourceType: 'P',
            csvError: true,
            csvType: 'F',
            description: `An error occurred while processing data for plot ${importFromFewsMessages[messageKey][0].plotId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} (workflow Test_Fluvial_Workflow5): Request failed with status code 400 (${badRequestMockResponse.response.data})`
          },
          expectedLocationData: [initialLocationData],
          expectedNumberOfImportedRecords: 1
        },
        {
          messageKey,
          expectedLocationData: [initialLocationData],
          expectedNumberOfImportedRecords: 1
        }
      ]

      const request = new sql.Request(pool)

      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndPartialDataIsImported(config[0])

      await request.batch(`
        update
          fff_staging.fluvial_display_group_workflow
        set
          location_ids = 'Test Location3a;Test Location3b'
        where
          workflow_id = 'Test_Fluvial_Workflow5' and
          plot_id = 'Test Fluvial Plot5'
      `)
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported(config[1].messageKey, 1)
    })
    it('should create a timeseries staging exception when one or more locations linked to a plot do not exist and allow replay following core engine resolution of the problem', async () => {
      const messageKey = 'singlePlotApprovedForecastWithMultipleLocations'
      const badRequestMockResponse = new Error('Request failed with status code 400')
      badRequestMockResponse.response = {
        data: 'Location Test Location3c does not exists Location Test Location3d does not exists',
        status: 400
      }

      const knownLocationsMockReponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }

      const initialLocationData = {
        plotId: importFromFewsMessages[messageKey][0].plotId,
        includedLocations: ['Test Location3a', 'Test Location3b'],
        excludedLocations: ['Test Location3c', 'Test Location3d']
      }

      const config = [
        {
          messageKey,
          mockResponses: [badRequestMockResponse, knownLocationsMockReponse],
          expectedErrorDetails: {
            sourceId: importFromFewsMessages[messageKey][0].plotId,
            sourceType: 'P',
            csvError: true,
            csvType: 'F',
            description: `An error occurred while processing data for plot ${importFromFewsMessages[messageKey][0].plotId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} (workflow Test_Fluvial_Workflow3): Request failed with status code 400 (${badRequestMockResponse.response.data})`
          },
          expectedLocationData: [initialLocationData],
          expectedNumberOfImportedRecords: 1
        },
        {
          messageKey,
          mockResponses: [
            {
              data: {
                key: 'Timeseries data'
              }
            }
          ],
          expectedLocationData: [
            {
              plotId: importFromFewsMessages[messageKey][0].plotId,
              includedLocations: ['Test Location3c', 'Test Location3d'],
              excludedLocations: ['Test Location3a', 'Test Location3b']
            },
            initialLocationData
          ],
          expectedNumberOfImportedRecords: 2
        }
      ]
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndPartialDataIsImported(config[0])
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config[1])
    })
    it('should create a timeseries staging exception when no locations linked to a plot exist', async () => {
      const messageKey = 'singlePlotApprovedForecastWithNoKnownLocations'
      const badRequestMockResponse = new Error('Request failed with status code 400')
      badRequestMockResponse.response = {
        data: 'Location Test Location3c does not exists Location Test Location3d does not exists',
        status: 400
      }
      const config = {
        messageKey,
        mockResponses: [badRequestMockResponse, badRequestMockResponse],
        expectedErrorDetails: {
          sourceId: importFromFewsMessages[messageKey][0].plotId,
          sourceType: 'P',
          csvError: true,
          csvType: 'F',
          description: `An error occurred while processing data for plot ${importFromFewsMessages[messageKey][0].plotId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} (workflow Test_Fluvial_Workflow4): Request failed with status code 400 (${badRequestMockResponse.response.data})`
        }
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(config)
    })
    it('should load a single plot associated with a workflow that is also associated with non display group data, inheriting the default offsets for the ndg data', async () => {
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
        spanWorkflowId: 'Span_Workflow2',
        mockResponses
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should load a single plot associated with a workflow that is also associated with non display group data, inheriting the specified offsets for the ndg data', async () => {
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
        messageKey: 'singlePlotAndFilterApprovedForecastCustomOffset',
        spanWorkflowId: 'Span_Workflow3',
        mockResponses
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
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 5000)
  })

  async function insertTimeseriesHeaders (pool) {
    const request = new sql.Request(pool)
    const earlierTaskRunStartTime = moment.utc(importFromFewsMessages.commonMessageData.startTime).subtract(30, 'seconds')
    const earlierTaskRunCompletionTime = moment.utc(importFromFewsMessages.commonMessageData.completionTime).subtract(30, 'seconds')
    await request.input('taskRunStartTime', sql.DateTime2, moment.utc(importFromFewsMessages.commonMessageData.startTime).toISOString())
    await request.input('taskRunCompletionTime', sql.DateTime2, moment.utc(importFromFewsMessages.commonMessageData.completionTime).toISOString())
    await request.input('earlierTaskRunStartTime', sql.DateTime2, earlierTaskRunStartTime.toISOString())
    await request.input('earlierTaskRunCompletionTime', sql.DateTime2, earlierTaskRunCompletionTime.toISOString())

    await request.batch(`
      insert into
        fff_staging.timeseries_header
          (task_start_time, task_completion_time, task_run_id, workflow_id, forecast, approved, message)
      values
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000001', 'Test_Fluvial_Workflow1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000002', 'Test_Fluvial_Workflow2', 1, 1, '{"input": "Test message"}'),
         (@earlierTaskRunStartTime, @earlierTaskRunCompletionTime, 'ukeafffsmc00:000000003', 'Test_Fluvial_Workflow1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000004a', 'Span_Workflow2', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000004b', 'Span_Workflow3', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000005', 'Test_Fluvial_Workflow3', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000006', 'Test_Fluvial_Workflow4', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000007', 'Test_Fluvial_Workflow5', 1, 1, '{"input": "Test message"}')
    `)
  }
})
