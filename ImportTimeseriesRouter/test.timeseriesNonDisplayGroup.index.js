module.exports = describe('Tests for import timeseries non-display groups', () => {
  const taskRunCompleteMessages = require('../testing/messages/task-run-complete/non-display-group-messages')
  const Context = require('../testing/mocks/defaultContext')
  const Connection = require('../Shared/connection-pool')
  const messageFunction = require('./index')
  const moment = require('moment')
  const axios = require('axios')
  const sql = require('mssql')

  let context
  jest.mock('axios')

  const jestConnection = new Connection()
  const pool = jestConnection.pool
  const request = new sql.Request(pool)

  describe('Message processing for non display group task run completion', () => {
    beforeAll(() => {
      return pool.connect()
    })

    beforeAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow`)
    })

    beforeAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
    })

    beforeAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow`)
    })

    beforeAll(() => {
      return request.batch(`
        insert into
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow (workflow_id, filter_id)
        values
          ('Test_Workflow1', 'Test Filter1')
      `)
    })

    beforeAll(() => {
      return request.batch(`
        insert into
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow (workflow_id, filter_id)
        values
          ('Test_Workflow2', 'Test Filter2a')
      `)
    })

    beforeAll(() => {
      return request.batch(`
        insert into
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow (workflow_id, filter_id)
        values
          ('Test_Workflow2', 'Test Filter2b')
      `)
    })

    beforeEach(() => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
    })

    beforeEach(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
    })

    beforeEach(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception`)
    })

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow`)
    })

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
    })

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
    })

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception`)
    })

    afterAll(() => {
      // Closing the DB connection allows Jest to exit successfully.
      return pool.close()
    })

    it('should import data for a single filter associated with a non-forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      await processMessageAndCheckImportedData('singleFilterNonForecast', [mockResponse])
      await processMessageAndCheckImportedData('singleFilterNonForecast', [mockResponse])
    })
    it('should import data for a single filter associated with a non-forecast regardless of message processing order', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      await processMessageAndCheckImportedData('singleFilterNonForecast', [mockResponse])
    })
    it('should import data for multiple filters associated with a non-forecast', async () => {
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
      await processMessageAndCheckImportedData('multipleFilterNonForecast', mockResponses)
    })
    it('should allow the default task run start and end times to be overridden using environment variables', async () => {
      const originalEnvironment = process.env
      try {
        process.env['FEWS_START_TIME_OFFSET_HOURS'] = 24
        process.env['FEWS_END_TIME_OFFSET_HOURS'] = 48
        const mockResponse = {
          data: {
            key: 'Timeseries non-display groups data'
          }
        }
        await processMessageAndCheckImportedData('singleFilterNonForecast', [mockResponse])
      } finally {
        process.env = originalEnvironment
      }
    })
    it('should create a staging exception for an unknown workflow', async () => {
      const unknownWorkflow = 'unknownWorkflow'
      const workflowId = taskRunCompleteMessages[unknownWorkflow].input.description.split(' ')[1]
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported(unknownWorkflow, `Missing PI Server input data for ${workflowId}`)
    })
    it('should create a staging exception for a missing workflow', async () => {
      const missingWorkflow = 'missingWorkflow'
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported(missingWorkflow, 'Missing PI Server input data for with')
    })
    it('should create a staging exception for a non-forecast without an approval status', async () => {
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported('forecastWithoutApprovalStatus', 'Unable to extract task run approval status from message')
    })
    it('should create a staging exception for a message containing the boolean false', async () => {
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported('booleanFalseMessage', 'Message must be either a string or a pure object')
    })
    it('should create a staging exception for a message containing the number 1', async () => {
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported('numericMessage', 'Message must be either a string or a pure object')
    })
    it('should create a staging exception for a non-forecast without an end time', async () => {
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported('forecastWithoutEndTime', 'Unable to extract task run completion date from message')
    })
    it('should throw an exception when the core engine PI server is unavailable', async () => {
      // If the core engine PI server is down messages are elgible for replay a certain number of times so check that
      // an exception is thrown to facilitate this process.
      const mockResponse = new Error('connect ECONNREFUSED mockhost')
      await processMessageAndCheckExceptionIsThrown('singleFilterNonForecast', mockResponse)
    })
    it('should create a staging exception when a core engine PI server resource is unavailable', async () => {
      // If a core engine PI server resource is unvailable (HTTP response code 404), messages are probably elgible for replay a certain number of times so
      // check that an exception is thrown to facilitate this process. If misconfiguration has occurred, the maximum number
      // of replays will be reached and the message will be transferred to a dead letter queue for manual intervetion.
      const mockResponse = new Error('Request failed with status code 404')
      await processMessageAndCheckExceptionIsThrown('singleFilterNonForecast', mockResponse)
    })
    it('should throw an exception when the non_display_group_workflow table is being refreshed', async () => {
      // If the non_display_group_workflow table is being refreshed messages are elgible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      await lockNonDisplayGroupTableAndCheckMessageCannotBeProcessed('singleFilterNonForecast', mockResponse)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    it('should not import data for duplicate task runs', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      await processMessage('singleFilterNonForecast', [mockResponse])
      await processMessageAndCheckNoDataIsImported('singleFilterNonForecast', 1)
    })
  })

  async function processMessage (messageKey, mockResponses) {
    if (mockResponses) {
      let mock = axios.get
      for (const mockResponse of mockResponses) {
        mock = mock.mockReturnValueOnce(mockResponse)
      }
    }
    await messageFunction(context, taskRunCompleteMessages[messageKey])
  }

  async function processMessageAndCheckImportedData (messageKey, mockResponses) {
    await processMessage(messageKey, mockResponses)
    const messageDescription = taskRunCompleteMessages[messageKey].input.description
    const messageDescriptionIndex = messageDescription.startsWith('Task run') ? 2 : 1
    const expectedTaskCompletionTime = moment(new Date(`${taskRunCompleteMessages['commonMessageData'].completionTime} UTC`))
    const expectedTaskRunId = taskRunCompleteMessages[messageKey].input.source
    const expectedWorkflowId = taskRunCompleteMessages[messageKey].input.description.split(' ')[messageDescriptionIndex]
    const receivedFewsData = []
    const receivedPrimaryKeys = []

    const result = await request.query(`
      select
        t.id,
        th.workflow_id,
        th.task_run_id,
        th.task_completion_time,
        th.start_time,
        th.end_time,
        t.fews_data
      from
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header th,
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries t
      where
        th.id = t.timeseries_header_id
    `)

    expect(result.recordset.length).toBe(mockResponses.length)

    // Database interaction is asynchronous so the order in which records are written
    // cannot be guaranteed.
    // To check if records have been persisted correctly, copy the timeseries data
    // retrieved from the database to an array and then check that the array contains
    // each expected mock timeseries.
    // To check if messages containing the primary keys of the timeseries records will be
    // sent to a queue/topic for reporting and visualisation purposes, copy the primary
    // keys retrieved from the database to an array and check that the ouput binding for
    // staged timeseries contains each expected primary key.
    for (const index in result.recordset) {
      // Check that data common to all timeseries has been persisted correctly.
      if (index === '0') {
        const taskCompletionTime = moment(result.recordset[index].task_completion_time)
        const startTime = moment(result.recordset[index].start_time)
        const endTime = moment(result.recordset[index].end_time)

        expect(taskCompletionTime.toISOString()).toBe(expectedTaskCompletionTime.toISOString())
        expect(result.recordset[index].task_run_id).toBe(expectedTaskRunId)
        expect(result.recordset[index].workflow_id).toBe(expectedWorkflowId)

        // Check that the persisted values for the forecast start time and end time are based within expected range of
        // the task completion time taking into acccount that the default values can be overridden by environment variables.
        const startTimeOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 12
        const endTimeOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120
        const expectedStartTime = moment(taskCompletionTime).subtract(startTimeOffsetHours, 'hours')
        const expectedEndTime = moment(taskCompletionTime).add(endTimeOffsetHours, 'hours')
        expect(startTime.toISOString()).toBe(expectedStartTime.toISOString())
        expect(endTime.toISOString()).toBe(expectedEndTime.toISOString())
      }
      receivedFewsData.push(JSON.parse(result.recordset[index].fews_data))
      receivedPrimaryKeys.push(result.recordset[index].id)
    }

    for (const mockResponse of mockResponses) {
      expect(receivedFewsData).toContainEqual(mockResponse.data)
    }

    for (const stagedTimeseries of context.bindings.stagedTimeseries) {
      expect(receivedPrimaryKeys).toContainEqual(stagedTimeseries.id)
    }
  }

  async function processMessageAndCheckNoDataIsImported (messageKey, expectedNumberOfRecords) {
    await processMessage(messageKey)
    await checkAmountOfDataImported(expectedNumberOfRecords || 0)
  }

  async function checkAmountOfDataImported (expectedNumberOfRecords) {
    const result = await request.query(`
      select
        count(t.id) as number
      from
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header th,
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries t
      where
        th.id = t.timeseries_header_id
    `)
    expect(result.recordset[0].number).toBe(expectedNumberOfRecords)
  }

  async function processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported (messageKey, expectedErrorDescription) {
    await processMessage(messageKey)
    const result = await request.query(`
      select
        top(1) description
      from
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception
      order by
        exception_time desc
    `)
    expect(result.recordset[0].description).toBe(expectedErrorDescription)
    await checkAmountOfDataImported(0)
  }

  async function processMessageAndCheckExceptionIsThrown (messageKey, mockErrorResponse) {
    axios.get.mockRejectedValue(mockErrorResponse)
    await expect(messageFunction(context, taskRunCompleteMessages[messageKey]))
      .rejects.toThrow(mockErrorResponse)
  }
  async function lockNonDisplayGroupTableAndCheckMessageCannotBeProcessed (messageKey, mockResponse) {
    let transaction
    const tableName = 'non_display_group_workflow'
    try {
      // Lock the timeseries table and then try and process the message.
      transaction = new sql.Transaction(pool)
      await transaction.begin()
      const request = new sql.Request(transaction)
      await request.batch(`
      INSERT INTO
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} (workflow_id,filter_id)
      values
      ('dummyWorkflow', 'dummyFilter')
    `)
      await expect(processMessage(messageKey, [mockResponse])).rejects.toBeTimeoutError(tableName)
    } finally {
      if (transaction._aborted) {
        context.log.warn('The transaction has been aborted.')
      } else {
        await transaction.rollback()
        context.log.warn('The transaction has been rolled back.')
      }
    }
  }
})
