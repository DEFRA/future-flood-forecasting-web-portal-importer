module.exports = describe('Tests for import timeseries display groups', () => {
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
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_non_display_group_workflow`)
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
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_non_display_group_workflow (workflow_id, filter_id)
        values
          ('Test_Workflow1', 'Test Filter1')
      `)
    })

    beforeAll(() => {
      return request.batch(`
        insert into
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_non_display_group_workflow (workflow_id, filter_id)
        values
          ('Test_Workflow2', 'Test Filter2a')
      `)
    })

    beforeAll(() => {
      return request.batch(`
        insert into
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_non_display_group_workflow (workflow_id, filter_id)
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

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_non_display_group_workflow`)
    })

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
    })

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
    })

    afterAll(() => {
      // Closing the DB connection allows Jest to exit successfully.
      return pool.close()
    })

    it('should import data for a single filter associated with an approved forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await processMessageAndCheckImportedData('singleFilterApprovedForecast', [mockResponse])
    })
    it('should import data for multiple filters associated with an approved forecast', async () => {
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
      await processMessageAndCheckImportedData('multipleFilterApprovedForecast', mockResponses)
    })
    it('should not import data for an unapproved forecast', async () => {
      await processMessageAndCheckNoDataIsImported('unapprovedForecast')
    })
    it('should import data for a forecast approved manually', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await processMessageAndCheckImportedData('forecastApprovedManually', [mockResponse])
    })
    it('should allow the default forecast start and end times to be overridden using environment variables', async () => {
      const originalEnvironment = process.env
      try {
        process.env['FEWS_START_TIME_OFFSET_HOURS'] = 24
        process.env['FEWS_END_TIME_OFFSET_HOURS'] = 48
        const mockResponse = {
          data: {
            key: 'Timeseries display groups data'
          }
        }
        await processMessageAndCheckImportedData('singleFilterApprovedForecast', [mockResponse])
      } finally {
        process.env = originalEnvironment
      }
    })
    it('should create a staging exception for an unknown workflow', async () => {
      const unknownWorkflow = 'unknownWorkflow'
      const workflowId = taskRunCompleteMessages[unknownWorkflow].input.description.split(' ')[1]
      await processMessageAndCheckStagingExceptionIsCreated(unknownWorkflow, `Missing PI Server input data for ${workflowId}`)
    })
    it('should create a staging exception for an invalid message', async () => {
      await processMessageAndCheckStagingExceptionIsCreated('forecastWithoutApprovalStatus', 'Unable to extract task run approval status from message')
    })
    it('should throw an exception when the core engine PI server is unavailable', async () => {
      // If the core engine PI server is down messages are elgible for replay a certain number of times so check that
      // an exception is thrown to facilitate this process.
      const mockResponse = new Error('connect ECONNREFUSED mockhost')
      await processMessageAndCheckExceptionIsThrown('singleFilterApprovedForecast', mockResponse)
    })
    it('should create a staging exception when a core engine PI server resource is unavailable', async () => {
      // If a core engine PI server resource is unvailable (HTTP response code 404), messages are probably elgible for replay a certain number of times so
      // check that an exception is thrown to facilitate this process. If misconfiguration has occurred, the maximum number
      // of replays will be reached and the message will be transferred to a dead letter queue for manual intervetion.
      const mockResponse = new Error('Request failed with status code 404')
      await processMessageAndCheckExceptionIsThrown('singleFilterApprovedForecast', mockResponse)
    })
    it('should throw an exception when the fluvial_non_display_group_workflow table is being refreshed', async () => {
      // If the fluvial_non_display_group_workflow table is being refreshed messages are elgible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await lockNonDisplayGroupTableAndCheckMessageCannotBeProcessed('singleFilterApprovedForecast', mockResponse)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
  })

  async function processMessage (messageKey, mockResponses) {
    if (mockResponses) {
      let mock = axios.get
      for (const mockResponse of mockResponses) {
        mock = mock.mockReturnValueOnce(mockResponse)
      }
    }
    await messageFunction(context, JSON.stringify(taskRunCompleteMessages[messageKey]))
  }

  async function processMessageAndCheckImportedData (messageKey, mockResponses) {
    await processMessage(messageKey, mockResponses)
    const messageDescription = taskRunCompleteMessages[messageKey].input.description
    const messageDescriptionIndex = messageDescription.startsWith('Task run') ? 2 : 1
    const expectedTaskCompletionTime = moment(taskRunCompleteMessages['commonMessageData'].completionTime)
    const expectedTaskId = taskRunCompleteMessages[messageKey].input.source
    const expectedWorkflowId = taskRunCompleteMessages[messageKey].input.description.split(' ')[messageDescriptionIndex]
    const receivedFewsData = []
    const receivedPrimaryKeys = []

    const result = await request.query(`
      select
        t.id,
        th.workflow_id,
        th.task_id,
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
        expect(result.recordset[index].task_id).toBe(expectedTaskId)
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

  async function processMessageAndCheckNoDataIsImported (messageKey) {
    await processMessage(messageKey)
    const result = await request.query(`
      select
        count(*) as number
      from
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries
    `)
    expect(result.recordset[0].number).toBe(0)
  }

  async function processMessageAndCheckStagingExceptionIsCreated (messageKey, expectedErrorDescription) {
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
  }

  async function processMessageAndCheckExceptionIsThrown (messageKey, mockErrorResponse) {
    axios.get.mockRejectedValue(mockErrorResponse)
    await expect(messageFunction(context, JSON.stringify(taskRunCompleteMessages[messageKey])))
      .rejects.toThrow(mockErrorResponse)
  }
  async function lockNonDisplayGroupTableAndCheckMessageCannotBeProcessed (messageKey, mockResponse) {
    let transaction
    const tableName = 'fluvial_non_display_group_workflow'
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
