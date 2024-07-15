const CommonCoastalTimeseriesTestUtils = require('../shared/common-coastal-timeseries-test-utils')
const importFromFewsMessages = require('./messages/coastal-display-group-messages')
const ImportFromFewsTestUtils = require('./import-from-fews-test-utils')
const { checkImportedData } = require('./display-group-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const moment = require('moment')
const sql = require('mssql')

module.exports = describe('Tests for import coastal timeseries display groups', () => {
  let context
  let importFromFewsTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonCoastalTimeseriesTestUtils = new CommonCoastalTimeseriesTestUtils(pool, importFromFewsMessages)

  describe('Message processing for coastal display group timeseries import', () => {
    beforeAll(async () => {
      const request = new sql.Request(pool)
      await commonCoastalTimeseriesTestUtils.beforeAll()
      await request.batch(`
        insert into
          fff_staging.non_display_group_workflow (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Span_Workflow', 'SpanFilter', 1, 10, 20, 'external_historical'),
          ('Span_Workflow_Default_Offset', 'SpanFilterDefaultOffsets', 1, 0, 0, 'external_historical'),
          ('Span_Workflow_Multiple_Offsets', 'Multiple Offsets Filter1', 1, 3, 5, 'external_historical'),
          ('Span_Workflow_Multiple_Offsets', 'Multiple Offsets Filter2', 1, 7, 9, 'external_historical')
      `)
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.importFromFews = []
      importFromFewsTestUtils = new ImportFromFewsTestUtils(context, pool, importFromFewsMessages, checkImportedData)
      await commonCoastalTimeseriesTestUtils.beforeEach()
      await insertTimeseriesHeadersAndTimeseriesStagingExceptions(pool)
    })
    afterAll(async () => {
      await commonCoastalTimeseriesTestUtils.afterAll()
    })
    it('should import data for a single plot associated with an approved forecast task run', async () => {
      const mockResponse = {
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries display groups data'
              }]
            }
          ]
        }
      }
      const config = {
        messageKey: 'singlePlotApprovedForecast',
        mockResponses: [mockResponse]
      }

      // Add a timeseries staging exception associated with the workflow being defined in the coastal and fluvial
      // display group CSV files to ensure it is deactivated.
      const exceptionTime = moment.utc(importFromFewsMessages.commonMessageData.completionTime).subtract(15, 'seconds')
      const request = new sql.Request(pool)
      await request.input('exceptionTime', sql.DateTimeOffset, exceptionTime.toISOString())

      await request.batch(`
        declare @id1 uniqueidentifier;
        select
          @id1 = id
        from
          fff_staging.timeseries_header
        where
          task_run_id = 'ukeafffsmc00:000000001';

        insert into
          fff_staging.timeseries_staging_exception
            (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description)
          values
            (@id1, 'Test Coastal Plot', 'P', 1, 'U', 'error_plot_fews_parameters', '{"taskRunId": "ukeafffsmc00:000000001", "plotId": "Test Coastal Plot"}', @id1, 'Error plot text');
     `)
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should not import duplicate timeseries', async () => {
      const messageKey = 'singlePlotApprovedForecast'
      const mockResponse = {
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries display groups data'
              }]
            }
          ]
        }
      }
      const config = {
        messageKey,
        mockResponses: [mockResponse]
      }

      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported(messageKey, 1)
    })
    it('should import data for multiple plots associated with an approved forecast task run', async () => {
      const mockResponses = [{
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'First plot timeseries display groups data'
              }]
            }
          ]
        }
      },
      {
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Second plot timeseries display groups data'
              }]
            }
          ]
        }
      }]
      const config = {
        messageKey: 'multiplePlotApprovedForecast',
        mockResponses
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should not import data for an unapproved forecast task run', async () => {
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('unapprovedWorkflowPlot')
    })
    it('should not import data for an approved out-of-date forecast task run', async () => {
      const mockResponse = {
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries display groups data'
              }]
            }
          ]
        }
      }
      const config = {
        messageKey: 'laterSinglePlotApprovedForecast',
        mockResponses: [mockResponse]
      }

      // Add a staging exception and timeseries staging exception to an obsolete forecast to ensure they are deactivated.
      const exceptionTime = moment.utc(importFromFewsMessages.commonMessageData.completionTime).subtract(15, 'seconds')
      const request = new sql.Request(pool)
      await request.input('exceptionTime', sql.DateTimeOffset, exceptionTime.toISOString())

      await request.batch(`
        declare @id1 uniqueidentifier;
        select
          @id1 = id
        from
          fff_staging.timeseries_header
        where
          task_run_id = 'ukeafffsmc00:000000003';

        insert into
          fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
        values
          ('taskRunId invalid message', 'Error', 'ukeafffsmc00:000000003', 'I', 'Test_Coastal_Workflow5', @exceptionTime);

        insert into
          fff_staging.timeseries_staging_exception
            (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description)
          values
            (@id1, 'Test Coastal Plot 1', 'P', 1, 'C', 'error_plot_fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "plotId": "Test Coastal Plot 1"}', @id1, 'Error plot text');
      `)
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('earlierSinglePlotApprovedForecast')
    })
    it('should allow the default forecast start-time and end-time to be overridden using environment variables', async () => {
      const originalEnvironment = process.env
      try {
        process.env.FEWS_DISPLAY_GROUP_START_TIME_OFFSET_HOURS = '24'
        process.env.FEWS_DISPLAY_GROUP_END_TIME_OFFSET_HOURS = '48'
        const mockResponse = {
          data: {
            timeSeries: [
              {
                header: {
                },
                events: [{
                  key: 'Timeseries display groups data'
                }]
              }
            ]
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
      await importFromFewsTestUtils.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported(messageKey, 'Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes')
    })
    it('should allow replay following correction of a task run message associated with a staging exception', async () => {
      const invalidMessageKey = 'invalidPlotAndFilterMessage'
      const mockResponse = {
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries display groups data'
              }]
            }
          ]
        }
      }
      const config = {
        messageKey: 'singlePlotApprovedForecast',
        mockResponses: [mockResponse]
      }
      await importFromFewsTestUtils.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported(invalidMessageKey, 'Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes')
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should create a staging exception when a message does not contain a task run ID ', async () => {
      const messageKey = 'missingTaskRunIdMessage'
      await importFromFewsTestUtils.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported(messageKey, 'Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes')
    })
    it('should create a staging exception when a message does not contain a plot or filter ID', async () => {
      const messageKey = 'missingTaskRunIdMessage'
      await importFromFewsTestUtils.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported(messageKey, 'Messages processed by the ImportFromFews endpoint require must contain taskRunId and either plotId or filterId attributes')
    })
    it('should create a timeseries staging exception when a message contains an unknown plot or filter ID', async () => {
      const messageKey = 'unknownPlotId'
      const config = {
        messageKey,
        expectedErrorDetails: {
          sourceId: importFromFewsMessages[messageKey][0].plotId,
          sourceType: 'P',
          csvError: true,
          csvType: 'U',
          description: `Unable to find locations for plot ${importFromFewsMessages[messageKey][0].plotId} of task run ukeafffsmc00:000000001 in any display group CSV`
        }
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(config)
    })
    it('should create a timeseries staging exception when a workflow plot is defined in multiple display group CSV files', async () => {
      const messageKey = 'workflowPlotDefinedInMultipleDisplayGroupCsvFiles'
      const config = {
        messageKey,
        expectedErrorDetails: {
          sourceId: importFromFewsMessages[messageKey][0].plotId,
          sourceType: 'P',
          csvError: true,
          csvType: 'U',
          description: `Found locations for plot ${importFromFewsMessages[messageKey][0].plotId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} in coastal and fluvial display group CSVs`
        }
      }
      const request = new sql.Request(pool)
      await request.batch(`
        insert into
          fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
        values
          ('Test_Coastal_Workflow4', 'Test Coastal Plot 4', 'Test Coastal Location 4')
    `)
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(config)
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
          description: `An error occurred while processing data for plot ${importFromFewsMessages[messageKey][0].plotId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} (workflow Test_Coastal_Workflow): Request failed with status code 404 (${mockResponse.response.data})`
        }
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(config)
    })
    it('should import data for a single plot associated with an approved forecast with an output binding set to active', async () => {
      const mockResponse = {
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries display groups data'
              }]
            }
          ]
        }
      }

      process.env.IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED = 'true' // in this case the build script would contain function.json with an output binding
      context.bindingDefinitions = [{ direction: 'out', name: 'stagedTimeseries', type: 'serviceBus' }]

      const config = {
        messageKey: 'singlePlotApprovedForecast',
        mockResponses: [mockResponse]
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should load a single plot (with the correct offset timings inherited) associated with a workflow that is also associated with non display group data', async () => {
      const mockResponses = [{
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries data'
              }]
            }
          ]
        }
      },
      {
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries data'
              }]
            }
          ]
        }
      }]

      const config = {
        messageKey: 'singlePlotAndFilterApprovedForecast',
        spanWorkflowId: 'Span_Workflow',
        mockResponses
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should load a single plot with the default ndg correct offset timings when timings are not specified in reference data for a workflow that is also associated with non display group data', async () => {
      const mockResponses = [{
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries data'
              }]
            }
          ]
        }
      },
      {
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries data'
              }]
            }
          ]
        }
      }]

      const config = {
        messageKey: 'singlePlotAndFilterApprovedForecastDefaultOffsets',
        spanWorkflowId: 'Span_Workflow_Default_Offset',
        mockResponses
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should perform a partial load for a task run if an error occurs for indiviual plots or filters', async () => {
      const messageKeyRoot = 'partialTaskRunLoad'
      const partOneMessageKey = `${messageKeyRoot}PartOne`
      const partTwoMessageKey = `${messageKeyRoot}PartTwo`
      const partThreeMessageKey = `${messageKeyRoot}PartThree`
      const badRequestMockResponse = new Error('Request failed with status code 400')
      badRequestMockResponse.response = {
        data: 'Error text',
        status: 400
      }
      const internalServerErrorMockResponse = new Error('Request failed with status code 500')
      internalServerErrorMockResponse.response = {
        data: 'Error text',
        status: 500
      }
      const config = [
        {
          messageKey: partOneMessageKey,
          mockResponses: [badRequestMockResponse, badRequestMockResponse],
          spanWorkflowId: 'Partial_Load_Span_Workflow',
          expectedErrorDetails: {
            sourceId: importFromFewsMessages[partOneMessageKey][0].plotId,
            sourceType: 'P',
            csvError: true,
            csvType: 'C',
            description: `An error occurred while processing data for plot ${importFromFewsMessages[partOneMessageKey][0].plotId} of task run ${importFromFewsMessages[partOneMessageKey][0].taskRunId} (workflow Partial_Load_Span_Workflow): Request failed with status code 400 (${badRequestMockResponse.response.data})`
          }
        },
        {
          messageKey: partTwoMessageKey,
          mockResponses: [{
            data: {
              timeSeries: [
                {
                  header: {
                  },
                  events: [{
                    key: 'Timeseries data'
                  }]
                }
              ]
            }
          },
          {
            data: {
              timeSeries: [
                {
                  header: {
                  },
                  events: [{
                    key: 'Timeseries data'
                  }]
                }
              ]
            }
          },
          {
            data: {
              timeSeries: [
                {
                  header: {
                  },
                  events: [{
                    key: 'Timeseries data'
                  }]
                }
              ]
            }
          }],
          spanWorkflowId: 'Partial_Load_Span_Workflow'
        },
        {
          messageKey: partThreeMessageKey,
          mockResponses: [internalServerErrorMockResponse],
          spanWorkflowId: 'Partial_Load_Span_Workflow',
          expectedErrorDetails: {
            sourceId: importFromFewsMessages[partThreeMessageKey][0].filterId,
            sourceType: 'F',
            csvError: false,
            csvType: null,
            description: `An error occurred while processing data for filter ${importFromFewsMessages[partThreeMessageKey][0].filterId} of task run ${importFromFewsMessages[partOneMessageKey][0].taskRunId} (workflow Partial_Load_Span_Workflow): Request failed with status code ${internalServerErrorMockResponse.response.status} (${internalServerErrorMockResponse.response.data})`
          },
          expectedNumberOfRecords: 3,
          expectedNumberOfTimeseriesStagingExceptionRecords: 2
        }
      ]

      const request = new sql.Request(pool)
      await request.batch(`
        insert into
          fff_staging.non_display_group_workflow
             (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Partial_Load_Span_Workflow', 'Test Span Filter 9a', 1, 0, 0, 'external_historical'),
          ('Partial_Load_Span_Workflow', 'Test Span Filter 9b', 1, 0, 0, 'external_historical')
      `)

      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(config[0])
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config[1])
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(config[2])
      // Provide a higher test timeout for this test.
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 5000)
    it('should throw an exception when the coastal_display_group_workflow table locks due to refresh', async () => {
      // If the coastal_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      const mockResponse = {
        data: {
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries display groups data'
              }]
            }
          ]
        }
      }
      await importFromFewsTestUtils.lockWorkflowTableAndCheckMessagesCannotBeProcessed('coastalDisplayGroupWorkflow', 'singlePlotApprovedForecast', mockResponse)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 5000)
    it('should create a timeseries staging exception for a spanning workflow plot with multiple different custom offsets specified', async () => {
      const messageKey = 'multipleOffsets'
      const config = {
        messageKey,
        expectedErrorDetails: {
          sourceId: importFromFewsMessages[messageKey][0].plotId,
          sourceType: 'P',
          csvError: true,
          csvType: 'C',
          description: 'An error has been found in the custom offsets for workflow: Span_Workflow_Multiple_Offsets. 2 found. Task run ukeafffsmc00:0000000011 in the non-display group CSV.'
        }
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(config)
    })
    it('should send a message for replay using default scheduling when custom scheduling is not configured, data returned from the PI Server has missing events and the maximum amount of time allowed for PI Server indexing to complete has not been exceeded', async () => {
      const config = {
        approved: 1,
        forecast: 1,
        messageKey: 'forecastWithMissingEvents',
        taskRunCompletionTimeOffsetMillis: 30000,
        taskRunId: 'ukeafffsmc00:000000012',
        taskRunStartTimeOffsetMillis: 30000,
        workflowId: 'Coastal_Missing_Event_Workflow'
      }

      await importFromFewsTestUtils.processMessageAndCheckMessageIsSentForReplay(config)
    })
    it('should send a message for replay using custom scheduling when custom scheduling is configured, data returned from the PI Server has missing events and the maximum amount of time allowed for PI Server indexing to complete has not been exceeded', async () => {
      process.env.CHECK_FOR_TASK_RUN_MISSING_EVENTS_DELAY_MILLIS = '5000'

      const config = {
        approved: 1,
        forecast: 1,
        messageKey: 'forecastWithMissingEvents',
        taskRunCompletionTimeOffsetMillis: 30000,
        taskRunId: 'ukeafffsmc00:000000012',
        taskRunStartTimeOffsetMillis: 30000,
        workflowId: 'Coastal_Missing_Event_Workflow'
      }

      await importFromFewsTestUtils.processMessageAndCheckMessageIsSentForReplay(config)
    })
    it('should import data for a single plot with no missing events for an approved forecast when the maximum amount of time allowed for PI Server indexing to complete has not been exceeded', async () => {
      const messageKey = 'currentSinglePlotApprovedForecast'

      const mockResponse = {
        data: {
          version: 'mock version number',
          timeZone: 'mock time zone',
          timeSeries: [
            {
              header: {
              },
              events: [{
                key: 'Timeseries display groups data'
              }]
            }
          ]
        }
      }
      const messageProcessingConfig = {
        messageKey,
        mockResponses: [mockResponse]
      }

      const timeseriesHeaderConfig = {
        approved: 1,
        forecast: 1,
        messageKey,
        taskRunCompletionTimeOffsetMillis: 30000,
        taskRunId: 'ukeafffsmc00:000000013',
        taskRunStartTimeOffsetMillis: 30000,
        workflowId: 'Coastal_No_Missing_Events_Workflow'
      }

      await importFromFewsTestUtils.insertCurrentTimeseriesHeader(timeseriesHeaderConfig)
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(messageProcessingConfig)
    })
    it('should import data for a single plot with no events for an approved forecast when the maximum amount of time allowed for PI Server indexing to complete has been exceeded', async () => {
      const mockResponse = {
        data: {
          timeSeries: [
            {
              header: {
              }
            }
          ]
        }
      }
      const config = {
        messageKey: 'singlePlotApprovedForecast',
        mockResponses: [mockResponse]
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should send a message for replay using default scheduling when custom scheduling is not configured, data returned from the PI Server has no events and the maximum amount of time allowed for PI Server indexing to complete has not been exceeded', async () => {
      const config = {
        approved: 1,
        forecast: 1,
        deleteEvents: true,
        messageKey: 'forecastWithMissingEvents',
        taskRunCompletionTimeOffsetMillis: 30000,
        taskRunId: 'ukeafffsmc00:000000012',
        taskRunStartTimeOffsetMillis: 30000,
        workflowId: 'Coastal_Missing_Event_Workflow'
      }

      await importFromFewsTestUtils.processMessageAndCheckMessageIsSentForReplay(config)
    })
  })

  async function insertTimeseriesHeadersAndTimeseriesStagingExceptions (pool) {
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
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000001', 'Test_Coastal_Workflow', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000002', 'Test_Coastal_Workflow2', 1, 1, '{"input": "Test message"}'),
         (@earlierTaskRunStartTime, @earlierTaskRunCompletionTime, 'ukeafffsmc00:000000003', 'Test_Coastal_Workflow1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000004', 'Test_Coastal_Workflow1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000005', 'Test_Ignored_Workflow_1', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000006', 'Test_Coastal_Workflow3', 1, 0, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000007', 'Span_Workflow', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000008', 'Test_Coastal_Workflow4', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000009', 'Partial_Load_Span_Workflow', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:0000000010', 'Span_Workflow_Default_Offset', 1, 1, '{"input": "Test message"}'),
         (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:0000000011', 'Span_Workflow_Multiple_Offsets', 1, 1, '{"input": "Test message"}')
    `)
  }
})
