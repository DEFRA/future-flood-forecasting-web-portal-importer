module.exports = describe('Tests for import timeseries non-display groups', () => {
  const dateFormat = 'YYYY-MM-DD HH:mm:ss'
  const importFromFewsMessages = require('./messages/non-display-group-messages')
  const Context = require('../mocks/defaultContext')
  const ConnectionPool = require('../../../Shared/connection-pool')
  const CommonNonDisplayGroupTimeseriesTestUtils = require('../shared/common-non-display-group-timeseries-test-utils')
  const ImportFromFewsTestUtils = require('./import-from-fews-test-utils')
  const moment = require('moment')
  const sql = require('mssql')

  let context
  let importFromFewsTestUtils
  jest.mock('axios')

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonNonDisplayGroupTimeseriesTestUtils = new CommonNonDisplayGroupTimeseriesTestUtils(pool, importFromFewsMessages)

  const SIMULATED_FORECASTING = 'simulated_forecasting'
  const EXTERNAL_FORECASTING = 'external_forecasting'
  const EXTERNAL_HISTORICAL = 'external_historical'

  const defaultTruncationOffsetHours = process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS'] ? parseInt(process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS']) : 24

  describe('Message processing for non-display group timeseries import', () => {
    beforeAll(async () => {
      const request = new sql.Request(pool)
      await commonNonDisplayGroupTimeseriesTestUtils.beforeAll(pool)
      await request.batch(`
        insert into
          fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
        values
          ('Span_Workflow', 'SpanPlot', 'Span Location' )
      `)
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      importFromFewsTestUtils = new ImportFromFewsTestUtils(context, pool, importFromFewsMessages, checkImportedData)
      await commonNonDisplayGroupTimeseriesTestUtils.beforeEach(pool)
      await insertTimeseriesHeaders(pool)
    })
    afterAll(async () => {
      await commonNonDisplayGroupTimeseriesTestUtils.afterAll(pool)
    })

    it('should import data for a single filter associated with a non-forecast task run', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      const config = {
        messageKey: 'singleFilterNonForecast',
        mockResponses: [mockResponse]
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should import data for multiple filters associated with a non-forecast task run', async () => {
      const mockResponses = [{
        data: {
          key: 'First filter timeseries non-display groups data'
        }
      },
      {
        data: {
          key: 'Second filter timeseries non-display groups data'
        }
      }]
      const config = {
        messageKey: 'multipleFilterNonForecast',
        mockResponses: mockResponses
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should import data for a single filter associated with an approved forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      const config = {
        messageKey: 'singleFilterApprovedForecast',
        mockResponses: [mockResponse]
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should import data for plots and filters associated with the same workflow', async () => {
      const displayMockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      const nonDisplayMockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      const config = {
        messageKey: 'filterAndPlotApprovedForecast',
        mockResponses: [displayMockResponse, nonDisplayMockResponse]
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should not import data for a task run that is out-of-date compared with data in staging ', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      const config = {
        messageKey: 'laterSingleFilterTaskRun',
        mockResponses: [mockResponse]
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('earlierSingleFilterTaskRun')
    })
    it('should create a staging exception when a timeseries header does not exist for a task run', async () => {
      const messageKey = 'unknownTaskRun'
      const taskRunId = importFromFewsMessages[messageKey][0].taskRunId
      await importFromFewsTestUtils.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported(messageKey, `Unable to retrieve TIMESERIES_HEADER record for task run ${taskRunId}`)
    })
    it('should create a timeseries staging exception when a message contains an unknown filter ID', async () => {
      const messageKey = 'unknownFilterId'
      const expectedErrorDetails = {
        sourceId: importFromFewsMessages[messageKey][0].filterId,
        sourceType: 'F',
        csvError: true,
        csvType: 'N',
        description: `Unable to find data for filter ${importFromFewsMessages[messageKey][0].filterId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} in the non-display group CSV`
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(messageKey, null, expectedErrorDetails)
    })
    it('should throw an exception when the core engine PI server is unavailable', async () => {
      // If the core engine PI server is down messages are eligible for replay a certain number of times so check that
      // an exception is thrown to facilitate this process.
      const mockResponse = new Error('connect ECONNREFUSED mockhost')
      await importFromFewsTestUtils.processMessagesAndCheckExceptionIsThrown('singleFilterNonForecast', mockResponse)
    })
    it('should create a timeseries staging exception when a core engine PI server resource is unavailable', async () => {
      const mockResponse = new Error('Request failed with status code 404')
      mockResponse.response = {
        data: 'Error text',
        status: 404
      }
      const messageKey = 'singleFilterNonForecast'
      const expectedErrorDetails = {
        sourceId: importFromFewsMessages[messageKey][0].filterId,
        sourceType: 'F',
        csvError: false,
        csvType: null,
        description: `An error occured while processing data for filter ${importFromFewsMessages[messageKey][0].filterId} of task run ${importFromFewsMessages[messageKey][0].taskRunId} (workflow Test_Workflow1): Request failed with status code 404 (${mockResponse.response.data})`
      }
      await importFromFewsTestUtils.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported(messageKey, [mockResponse], expectedErrorDetails)
    })
    it('should throw an exception when the non_display_group_workflow table locks due to refresh', async () => {
      // If the non_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      await importFromFewsTestUtils.lockWorkflowTableAndCheckMessagesCannotBeProcessed('nonDisplayGroupWorkflow', 'singleFilterNonForecast', mockResponse)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    // it('should not import data for duplicate task runs', async () => {
    //   const mockResponse = {
    //     data: {
    //       key: 'Timeseries non-display groups data'
    //     }
    //   }
    //   await importFromFewsTestUtils.processMessages('singleFilterNonForecast', [mockResponse])
    //   await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('singleFilterNonForecast', 1)
    // })
    it('should use previous task run end time as creation start time for a single filter associated with a non-forecast', async () => {
      const mockResponses = [
        {
          data: {
            key: 'Timeseries non-display groups data'
          }
        },
        {
          data: {
            key: 'Timeseries non-display groups data'
          }
        }
      ]
      const config = [
        {
          messageKey: 'singleFilterNonForecast',
          mockResponses: [mockResponses[0]]
        },
        {
          messageKey: 'singleFilterNonForecast',
          mockResponses: [mockResponses[1]]
        }
      ]

      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config[0])
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config[1])
    })
    it('should adopt the start-time-offset environment setting for a single filter associated with a non-forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }

      process.env.FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS = 10

      const config = {
        messageKey: 'singleFilterNonForecast',
        mockResponses: [mockResponse],
        overrideValues: {
          backward: -10
        }
      }

      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should import data for a single filter associated with a non-forecast and with output binding set to true, check timeseries id has been captured in output binding', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }

      const config = {
        messageKey: 'singleFilterNonForecast',
        mockResponses: [mockResponse]
      }

      process.env.IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED = true // in this case the build script would contain function.json with an output binding
      context.bindingDefinitions = [{ direction: 'out', name: 'stagedTimeseries', type: 'servieBus' }]
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should import data for a single filter associated with custom time period offsets', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }

      const config = {
        messageKey: 'singleFilterApprovedForecastCustomOffset',
        mockResponses: [mockResponse],
        overrideValues: {
          forward: 20,
          backward: -10
        }
      }

      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should load a single filter associated with a workflow that is also associated with display group data', async () => {
      const mockResponses = [{
        data: {
          key: 'Timeseries data'
        }
      },
      {
        data: {
          key: 'Timeseries data'
        }
      }]

      const config = {
        messageKey: 'filterAndPlotApprovedForecast',
        mockResponses: mockResponses
      }
      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should import data for a single filter associated with a simulated-forecast data and ensure the query parameters contain only start/end times (no start/end creation times)', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }

      const config = {
        messageKey: 'singleFilterApprovedSimulatedForecast',
        mockResponses: [mockResponse],
        overrideValues: {
          timeseriesType: SIMULATED_FORECASTING
        }
      }

      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should import data for a single filter associated with external historical data and ensure the query parameters contain start/end times and start/end creation times', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }

      const config = {
        messageKey: 'singleFilterApprovedExternalHistorical',
        mockResponses: [mockResponse],
        overrideValues: {
          timeseriesType: EXTERNAL_HISTORICAL
        }
      }

      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should import data for a single filter associated with external forecasting data and ensure the query parameters contain start/end times and start/end creation times', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }

      const config = {
        messageKey: 'singleFilterApprovedExternalForecasting',
        mockResponses: [mockResponse],
        overrideValues: {
          timeseriesType: EXTERNAL_FORECASTING
        }
      }

      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should not import data for an unknown timeseries type', async () => {
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('workflowUnknownTimeseriesType')
    })
    it('should import data for an approved:false message associated with a timeseries type not requiring approval', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }

      const config = {
        messageKey: 'unapprovedExternalForecasting',
        mockResponses: [mockResponse]
      }

      await importFromFewsTestUtils.processMessagesAndCheckImportedData(config)
    })
    it('should not import data for an approved:false message associated with a timeseries type requiring approval', async () => {
      await importFromFewsTestUtils.processMessagesAndCheckNoDataIsImported('unapprovedSimulatedForecast')
    })
  })

  async function insertTimeseriesHeaders (pool) {
    const request = new sql.Request(pool)
    const earlierTaskRunStartTime = moment.utc(importFromFewsMessages.commonMessageData.startTime).subtract(30, 'seconds')
    const earlierTaskRunCompletionTime = moment.utc(importFromFewsMessages.commonMessageData.completionTime).subtract(30, 'seconds')
    const laterTaskRunStartTime = moment.utc(importFromFewsMessages.commonMessageData.startTime).add(30, 'seconds')
    const laterTaskRunCompletionTime = moment.utc(importFromFewsMessages.commonMessageData.completionTime).add(30, 'seconds')
    await request.input('taskRunStartTime', sql.DateTime2, importFromFewsMessages.commonMessageData.startTime)
    await request.input('taskRunCompletionTime', sql.DateTime2, importFromFewsMessages.commonMessageData.completionTime)
    await request.input('earlierTaskRunStartTime', sql.DateTime2, earlierTaskRunStartTime.format(dateFormat))
    await request.input('earlierTaskRunCompletionTime', sql.DateTime2, earlierTaskRunCompletionTime.format(dateFormat))
    await request.input('laterTaskRunStartTime', sql.DateTime2, laterTaskRunStartTime.format(dateFormat))
    await request.input('laterTaskRunCompletionTime', sql.DateTime2, laterTaskRunCompletionTime.format(dateFormat))

    await request.batch(`
      insert into
        fff_staging.timeseries_header
          (task_start_time, task_completion_time, task_run_id, workflow_id, forecast, approved, message)
      values
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000001', 'Test_Workflow1', 0, 0, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000002', 'Test_Workflow2', 0, 0, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000003', 'Test_Workflow3', 1, 1, '{"input": "Test message"}'),
       (@earlierTaskRunStartTime, @earlierTaskRunCompletionTime, 'ukeafffsmc00:000000004', 'Test_Workflow4', 1, 1, '{"input": "Test message"}'),
       (@laterTaskRunStartTime, @laterTaskRunCompletionTime, 'ukeafffsmc00:000000005', 'Test_Workflow4', 1, 1, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000006', 'Test_Ignored_Workflow_1', 1, 1, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000007', 'Test_Workflow5', 1, 0, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000008', 'Test_workflowCustomTimes', 1, 1, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000009', 'Simulated_Forecasting_Workflow1', 1, 1, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000010', 'External_Forecasting_Workflow1', 1, 1, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000011', 'External_Historical_Workflow', 1, 1, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000012', 'Simulated_Forecasting_Workflow2', 1, 0, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000013', 'Span_Workflow', 1, 1, '{"input": "Test message"}'),
       (@taskRunStartTime, @taskRunCompletionTime, 'ukeafffsmc00:000000014', 'External_Forecasting_Workflow2', 1, 0, '{"input": "Test message"}')
    `)
  }

  async function checkImportedData (config, context, pool) {
    const receivedFewsData = []
    const receivedPrimaryKeys = []
    const taskRunId = importFromFewsMessages[config.messageKey][0].taskRunId
    const previousTaskRunEndTimeRequest = new sql.Request(pool)
    const currentTaskRunCompletionTimeseriesRequest = new sql.Request(pool)

    await previousTaskRunEndTimeRequest.input('taskRunId', sql.VarChar, taskRunId)
    const previousTaskRunEndTimeResult = await previousTaskRunEndTimeRequest.query(`
      select
        max(task_completion_time) as previous_task_run_end_time 
      from
        fff_staging.timeseries_header
      where
        task_run_id <> @taskRunId and
        workflow_id in (
          select
            workflow_id
          from
            fff_staging.timeseries_header th
          where
            task_run_id = @taskRunId
        )    
    `)

    const previousTaskRunEndTime = previousTaskRunEndTimeResult.recordset[0].previous_task_run_end_time

    await currentTaskRunCompletionTimeseriesRequest.input('taskRunId', sql.VarChar, taskRunId)
    const result = await currentTaskRunCompletionTimeseriesRequest.query(`
      select
        t.id,
        t.fews_parameters,
        th.workflow_id,
        th.task_run_id,
        th.task_start_time,
        th.task_completion_time,
        cast(decompress(t.fews_data) as varchar(max)) as fews_data,
        convert(bit, case
          when t.fews_parameters like '&filterId=%' then 1
          else 0
          end
        ) as is_filter  
      from
        fff_staging.timeseries_header th,
        fff_staging.timeseries t
      where
        th.id = t.timeseries_header_id and
        th.workflow_id in
        (
          select
            workflow_id
          from
            fff_staging.timeseries_header th
          where
            task_run_id = @taskRunId   
        )
    `)

    expect(result.recordset.length).toBe(config.mockResponses.length)

    let taskRunCompletionTime
    for (const index in result.recordset) {
      // Database interaction is asynchronous so the order in which records are written
      // cannot be guaranteed.
      // To check if records have been persisted correctly, copy the timeseries data
      // retrieved from the database to an array and then check that the array contains
      // each expected mock timeseries.
      // To check if messages containing the primary keys of the timeseries records will be
      // sent to a queue/topic for reporting and visualisation purposes, copy the primary
      // keys retrieved from the database to an array and check that the ouput binding for
      // staged timeseries contains each expected primary key.
      receivedFewsData.push(JSON.parse(result.recordset[index].fews_data))
      receivedPrimaryKeys.push(result.recordset[index].id)

      // Check that filter timeseries data has been persisted correctly (plot timeseries data is checked in other unit tests).
      if (result.recordset[index].is_filter) {
        taskRunCompletionTime = moment(result.recordset[index].task_completion_time)

        // Check that the persisted values for the forecast start time and end time are based within expected range of
        // the task run completion time taking into acccount that the default values can be overridden by environment variables.
        let expectedStartTime
        if (previousTaskRunEndTime) {
          expectedStartTime = previousTaskRunEndTime
        } else {
          expectedStartTime = moment(result.recordset[index].task_start_time)
        }

        const expectedEndTime = moment(taskRunCompletionTime)

        let expectedOffsetStartTime
        let expectedOffsetEndTime
        if (config.overrideValues && (config.overrideValues.backward || config.overrideValues.forward)) {
          expectedOffsetStartTime = moment(expectedStartTime).subtract(Math.abs(config.overrideValues.backward), 'hours')
          expectedOffsetEndTime = moment(expectedEndTime).add(Math.abs(config.overrideValues.forward), 'hours')
        } else {
          expectedOffsetStartTime = moment(expectedStartTime).subtract(defaultTruncationOffsetHours, 'hours')
          expectedOffsetEndTime = expectedEndTime
        }

        // Check fews parameters have been captured correctly.
        if (config.overrideValues && config.overrideValues.timeseriesType === SIMULATED_FORECASTING) {
          expect(result.recordset[index].fews_parameters).toContain(`&startTime=${expectedOffsetStartTime.toISOString().substring(0, 19)}Z`)
          expect(result.recordset[index].fews_parameters).toContain(`&endTime=${expectedOffsetEndTime.toISOString().substring(0, 19)}Z`)
          expect(result.recordset[index].fews_parameters).not.toContain('Creation')
        } else {
          expect(result.recordset[index].fews_parameters).toContain(`&startTime=${expectedOffsetStartTime.toISOString().substring(0, 19)}Z`)
          expect(result.recordset[index].fews_parameters).toContain(`&endTime=${expectedOffsetEndTime.toISOString().substring(0, 19)}Z`)
          expect(result.recordset[index].fews_parameters).toContain(`&startCreationTime=${expectedStartTime.toISOString().substring(0, 19)}Z`)
          expect(result.recordset[index].fews_parameters).toContain(`&endCreationTime=${expectedEndTime.toISOString().substring(0, 19)}Z`)
        }
      }
    }
    // Check that all the expected mocked data is loaded
    for (const mockResponse of config.mockResponses) {
      expect(receivedFewsData).toContainEqual(mockResponse.data)
    }

    // The following check is for when there is an output binding named 'stagedTimeseries' active
    if (process.env.IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED === true) {
      for (const stagedTimeseries of context.bindings.stagedTimeseries) {
        expect(receivedPrimaryKeys).toContainEqual(stagedTimeseries.id)
      }
    }
  }
})
