const CommonNonDisplayGroupTimeseriesTestUtils = require('../shared/common-non-display-group-timeseries-test-utils')
const taskRunCompleteMessages = require('./messages/task-run-complete/non-display-group-messages')
const ProcessFewsEventCodeTestUtils = require('./process-fews-event-code-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const moment = require('moment')
const sql = require('mssql')

jest.mock('@azure/service-bus')

module.exports = describe('Tests for import timeseries non-display groups', () => {
  let context
  let processFewsEventCodeTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const commonNonDisplayGroupTimeseriesTestUtils = new CommonNonDisplayGroupTimeseriesTestUtils(pool)
  const request = new sql.Request(pool)

  const expectedData = {
    singleFilterTaskRun: {
      forecast: true,
      approved: true,
      outgoingFilterIds: ['Test Filter3']
    },
    earlierSingleFilterTaskRun: {
      forecast: true,
      approved: true,
      outgoingFilterIds: ['Test Filter3']
    },
    singleFilterNonForecast: {
      forecast: false,
      approved: false,
      outgoingFilterIds: ['Test Filter1'],
      expectedNumberOfStagingExceptions: 1
    },
    multipleFilterNonForecast: {
      forecast: false,
      approved: false,
      outgoingFilterIds: ['Test Filter2a', 'Test Filter2b']
    },
    singleFilterApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingFilterIds: ['Test Filter1']
    },
    filterAndPlotApprovedForecast: {
      forecast: true,
      approved: true,
      outgoingPlotIds: ['SpanPlot'],
      outgoingFilterIds: ['SpanFilter']
    }
  }

  describe('Message processing for non display group task run completion', () => {
    beforeAll(async () => {
      await commonNonDisplayGroupTimeseriesTestUtils.beforeAll(pool)
      await request.batch(`
        insert into
          fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
        values
          ('Test_Workflow4', 'Test Plot4', 'Test Location4'),
          ('Span_Workflow', 'SpanPlot', 'Span Location' )
      `)
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.importFromFews = []
      processFewsEventCodeTestUtils = new ProcessFewsEventCodeTestUtils(context, pool, taskRunCompleteMessages)
      await commonNonDisplayGroupTimeseriesTestUtils.beforeEach(pool)
      const azureServiceBus = require('@azure/service-bus')
      azureServiceBus.ServiceBusAdministrationClient = jest.fn().mockImplementation(() => {
        return {
          getQueue: async queueName => {
            return new Promise((resolve, reject) => {
              resolve({
                maxDeliveryCount: 2
              })
            })
          }
        }
      })
    })

    afterAll(async () => {
      await commonNonDisplayGroupTimeseriesTestUtils.afterAll(pool)
    })

    it('should create a timeseries header and create a message for a single filter associated with a non-forecast task run', async () => {
      // Create a staging exception associated with an earlier task run of the workflow. It should not be deactivated.
      const exceptionTime = moment.utc(taskRunCompleteMessages.commonMessageData.completionTime).subtract(15, 'seconds')
      await request.input('exceptionTime', sql.DateTimeOffset, exceptionTime.toISOString())
      await request.query(`
        insert into
          fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
        values
          ('description: invalid message', 'Error', 'ukeafffsmc00:000000003', 'P', 'Test_Workflow1', @exceptionTime);
      `)
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
    it('should create a timeseries header and create messages for an approved forecast workflow task run associated with a single plot and a single filter', async () => {
      const messageKey = 'filterAndPlotApprovedForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
    })
    it('should ignore an approved out of date forecast task run', async () => {
      const messageKey = 'singleFilterTaskRun'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey])
      context.bindings.importFromFews = [] // reset the context bindings as it stays in test memory
      const expectedNumberOfHeaderRecords = 1
      const expectedNumberOfNewOutgoingMessages = 0
      await processFewsEventCodeTestUtils.processMessageAndCheckNoDataIsCreated('earlierSingleFilterTaskRun', expectedNumberOfHeaderRecords, expectedNumberOfNewOutgoingMessages)
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
      await processFewsEventCodeTestUtils.lockWorkflowTableAndCheckMessageCannotBeProcessed('nonDisplayGroupWorkflow', 'singleFilterApprovedForecast')
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 5000)
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
    it('should send a message for replay after a default pause when the PI Server indicates that all data for a task run is not available and the maximum number of replays has not been exceeded', async () => {
      // Use the default delay when checking if all task run data is available from the
      // PI Server to increase test coverage.
      delete process.env.CHECK_FOR_TASK_RUN_DATA_AVAILABILITY_DELAY_MILLIS
      const expectedError = new Error('All data is not available for task run ukeafffsmc00:000000001 (workflow Test_Workflow1)')
      const mockResponse = {
        data: 'Partial response data',
        status: 206
      }

      const messageKey = 'singleFilterNonForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckExceptionIsThrown(messageKey, expectedError, mockResponse)
    })
    it('should send a message for replay after a customised pause when the PI Server indicates that all data for a task run is not available and the maximum number of replays has not been reached', async () => {
      const expectedError = new Error('All data is not available for task run ukeafffsmc00:000000001 (workflow Test_Workflow1)')
      const mockResponse = {
        data: 'Partial response data',
        status: 206
      }

      const messageKey = 'singleFilterNonForecast'
      await processFewsEventCodeTestUtils.processMessageAndCheckExceptionIsThrown(messageKey, expectedError, mockResponse)
      // Perform the test again to provide test coverage for lazy instantiation of ServiceBusAdministrationClient.
      await processFewsEventCodeTestUtils.processMessageAndCheckExceptionIsThrown(messageKey, expectedError, mockResponse)
    })
    it('should create a timeseries header and create a message for a single filter based task run after a customised pause when the PI Server indicates that all data for the task run is not available and the maximum number of replays has been reached', async () => {
      const mockResponse = {
        data: 'Partial response data',
        status: 206
      }
      context.bindingData.deliveryCount = 1
      const messageKey = 'singleFilterTaskRun'
      await processFewsEventCodeTestUtils.processMessageAndCheckDataIsCreated(messageKey, expectedData[messageKey], null, mockResponse)
    })
    it('should create a staging exception when the PI Server returns a HTTP 206 status code and a Content-Range header', async () => {
      const messageKey = 'singleFilterNonForecast'
      const mockResponse = {
        headers: { 'Content-Range': 'Mock content range' },
        data: 'Partial response data',
        status: 206
      }
      const expectedErrorDescription = 'Received unexpected Content-Range header when checking PI Server data availability for task run ukeafffsmc00:000000001'
      await processFewsEventCodeTestUtils.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated(messageKey, expectedErrorDescription, mockResponse)
    })
  })
})
