module.exports = describe('Tests for import timeseries non-display groups', () => {
  const taskRunCompleteMessages = require('../testing/messages/task-run-complete/non-display-group-messages')
  const Context = require('../testing/mocks/defaultContext')
  const Connection = require('../Shared/connection-pool')
  const { objectToStream } = require('../testing/utils')
  const messageFunction = require('./index')
  const moment = require('moment')
  const axios = require('axios')
  const sql = require('mssql')

  let context
  jest.mock('axios')

  const jestConnection = new Connection()
  const pool = jestConnection.pool
  const request = new sql.Request(pool)
  const defaultTruncationOffsetHours = process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS'] ? parseInt(process.env['FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS']) : 24

  describe('Message processing for non display group task run completion', () => {
    beforeAll(async () => {
      await pool.connect()
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow`)
      await request.batch(`
        insert into
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow
             (workflow_id, filter_id, approved, forecast)
        values
          ('Test_Workflow1', 'Test Filter1', 0, 0),
          ('Test_Workflow2', 'Test Filter2a', 0, 0),
          ('Test_Workflow2', 'Test Filter2b', 0, 0),
          ('Test_Workflow3', 'Test Filter3', 0, 1),
          ('Test_Workflow4', 'Test Filter4', 0, 1)
      `)
      await request.batch(`
        insert into
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
        values
          ('Test_Workflow4', 'Test Plot4', 'Test Location4')
      `)
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception`)
    })

    afterAll(async () => {
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception`)
      // Closing the DB connection allows Jest to exit successfully.
      await pool.close()
    })
    it('should import data for a single filter associated with a non-forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
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
      await checkAmountOfDataImported(2)
    })
    it('should import data for a single filter associated with an approved forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      await processMessageAndCheckImportedData('singleFilterApprovedForecast', [mockResponse])
    })
    it('should import data for a single filter associated with an unapproved forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }
      await processMessageAndCheckImportedData('singleFilterUnapprovedForecast', [mockResponse])
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
      await processMessage('singlePlotAndFilterApprovedForecast', [displayMockResponse, nonDisplayMockResponse])
      await checkAmountOfDataImported(2)
    })
    it('should not import data for an out of date forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await processMessageAndCheckImportedData('singleFilterApprovedForecast', [mockResponse])
      await processMessageAndCheckNoDataIsImported('earlierSingleFilterApprovedForecast', 1)
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
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported('nonForecastWithoutApprovalStatus', 'Unable to extract task run approval status from message')
    })
    it('should create a staging exception for a message containing the boolean false', async () => {
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported('booleanFalseMessage', 'Message must be either a string or a pure object')
    })
    it('should create a staging exception for a message containing the number 1', async () => {
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported('numericMessage', 'Message must be either a string or a pure object')
    })
    it('should create a staging exception for a non-forecast without an end time', async () => {
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported('nonForecastWithoutEndTime', 'Unable to extract task run completion date from message')
    })
    it('should throw an exception when the core engine PI server is unavailable', async () => {
      // If the core engine PI server is down messages are eligible for replay a certain number of times so check that
      // an exception is thrown to facilitate this process.
      const mockResponse = new Error('connect ECONNREFUSED mockhost')
      await processMessageAndCheckExceptionIsThrown('singleFilterNonForecast', mockResponse)
    })
    it('should create a staging exception when a core engine PI server resource is unavailable', async () => {
      // If a core engine PI server resource is unvailable (HTTP response code 404), messages are probably eligible for replay a certain number of times so
      // check that an exception is thrown to facilitate this process. If misconfiguration has occurred, the maximum number
      // of replays will be reached and the message will be transferred to a dead letter queue for manual intervetion.
      const mockResponse = new Error('Request failed with status code 404')
      await processMessageAndCheckExceptionIsThrown('singleFilterNonForecast', mockResponse)
    })
    it('should throw an exception when the non_display_group_workflow table is being refreshed', async () => {
      // If the non_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
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
    it('should use previous task run end time as creation start time for a single filter associated with a non-forecast', async () => {
      const mockResponse = [{
        data: {
          key: 'Timeseries non-display groups data'
        }
      }, {
        data: {
          key: 'Timeseries non-display groups data'
        }
      }]
      const workflowAlreadyRan = {
        flag: true,
        length: 2
      }

      await processMessage('singleFilterNonForecast', [mockResponse[0]])
      await processMessageAndCheckImportedData('laterSingleFilterNonForecast', [mockResponse[1]], workflowAlreadyRan)
    })
    it('should adopt the start-time-offset environment setting for a single filter associated with a non-forecast', async () => {
      const mockResponse = [{
        data: {
          key: 'Timeseries non-display groups data'
        }
      }]

      process.env.FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS = 10
      const expectedOffsetHours = 10
      const workflowAlreadyRan = false
      await processMessageAndCheckImportedData('singleFilterNonForecast', mockResponse, workflowAlreadyRan, expectedOffsetHours)
    })
  })

  async function processMessage (messageKey, mockResponses) {
    if (mockResponses) {
      let mock = axios
      for (const mockResponse of mockResponses) {
        mock = mock.mockReturnValueOnce({ data: await objectToStream(mockResponse.data) })
      }
    }
    await messageFunction(context, taskRunCompleteMessages[messageKey])
  }

  async function processMessageAndCheckImportedData (messageKey, mockResponses, workflowAlreadyRan, offsetOverride) {
    // This function interrogates the most recent database row insert and all inserted test data payloads are checked.
    await processMessage(messageKey, mockResponses)
    const messageDescription = taskRunCompleteMessages[messageKey].input.description
    const messageDescriptionIndex = messageDescription.startsWith('Task run') ? 2 : 1
    const expectedTaskRunStartTime = moment(new Date(`${taskRunCompleteMessages[messageKey].input.startTime} UTC`))
    const expectedTaskRunCompletionTime = moment(new Date(`${taskRunCompleteMessages[messageKey].input.endTime} UTC`))
    const expectedTaskRunId = taskRunCompleteMessages[messageKey].input.source
    const expectedWorkflowId = taskRunCompleteMessages[messageKey].input.description.split(' ')[messageDescriptionIndex]
    const receivedFewsData = []
    const receivedPrimaryKeys = []

    const result = await request.query(`
      select
        t.id,
        t.fews_parameters,
        th.workflow_id,
        th.task_run_id,
        th.task_completion_time,
        th.start_time,
        th.end_time,
        th.message,
        cast(decompress(t.fews_data) as varchar(max)) as fews_data
      from
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header th,
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries t
      where
        th.id = t.timeseries_header_id
      order by
        th.import_time desc
    `)

    if (workflowAlreadyRan) {
      expect(result.recordset.length).toBe(workflowAlreadyRan.length)
    } else {
      expect(result.recordset.length).toBe(mockResponses.length)
    }

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
      // The most recent record is first in the array
      if (index === '0') {
        const taskRunCompletionTime = moment(result.recordset[index].task_completion_time)
        const startTime = moment(result.recordset[index].start_time)
        const endTime = moment(result.recordset[index].end_time)

        expect(taskRunCompletionTime.toISOString()).toBe(expectedTaskRunCompletionTime.toISOString())
        expect(result.recordset[index].task_run_id).toBe(expectedTaskRunId)
        expect(result.recordset[index].workflow_id).toBe(expectedWorkflowId)

        // Check that the persisted values for the forecast start time and end time are based within expected range of
        // the task run completion time taking into acccount that the default values can be overridden by environment variables.
        let expectedStartTime
        if (workflowAlreadyRan) {
          const previousEndTime = moment(result.recordset[1].end_time)
          expectedStartTime = previousEndTime
        } else {
          expectedStartTime = moment(expectedTaskRunStartTime)
        }
        const expectedEndTime = moment(expectedTaskRunCompletionTime)

        expect(startTime.toISOString()).toBe(expectedStartTime.toISOString())
        expect(endTime.toISOString()).toBe(expectedEndTime.toISOString())

        let expectedOffsetStartTime
        if (offsetOverride) {
          expectedOffsetStartTime = moment(expectedStartTime).subtract(offsetOverride, 'hours')
        } else {
          expectedOffsetStartTime = moment(expectedStartTime).subtract(defaultTruncationOffsetHours, 'hours')
        }

        // Check fews parameters have been captured correctly.
        expect(result.recordset[index].fews_parameters).toContain(`&startCreationTime=${expectedStartTime.toISOString().substring(0, 19)}Z`)
        expect(result.recordset[index].fews_parameters).toContain(`&startTime=${expectedOffsetStartTime.toISOString().substring(0, 19)}Z`)
        expect(result.recordset[index].fews_parameters).toContain(`&endTime=${expectedEndTime.toISOString().substring(0, 19)}Z`)
        expect(result.recordset[index].fews_parameters).toContain(`&endCreationTime=${expectedEndTime.toISOString().substring(0, 19)}Z`)

        // Check the incoming message has been captured correctly.
        expect(JSON.parse(result.recordset[index].message)).toEqual(taskRunCompleteMessages[messageKey])
      }

      receivedFewsData.push(JSON.parse(result.recordset[index].fews_data))
      receivedPrimaryKeys.push(result.recordset[index].id)
    }

    // Check that all the expected mocked data is loaded
    for (const mockResponse of mockResponses) {
      expect(receivedFewsData).toContainEqual(mockResponse.data)
    }

    // The following check is for when there is an output binding named 'stagedTimeseries' active.
    // for (const stagedTimeseries of context.bindings.stagedTimeseries) {
    //   expect(receivedPrimaryKeys).toContainEqual(stagedTimeseries.id)
    // }
  }

  async function processMessageAndCheckNoDataIsImported (messageKey, expectedNumberOfRecords) {
    await processMessage(messageKey)
    await checkAmountOfDataImported(expectedNumberOfRecords || 0)
  }

  async function checkAmountOfDataImported (expectedNumberOfRecords) {
    const result = await request.query(`
    select
      count(t.id)
    as
      number
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
    const expectedTaskRunId = taskRunCompleteMessages[messageKey].input ? taskRunCompleteMessages[messageKey].input.source : null
    const result = await request.query(`
    select top(1)
      payload,
      task_run_id,
      description
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception
    order by
      exception_time desc
    `)

    // Check the problematic message has been captured correctly.
    expect(JSON.parse(result.recordset[0].payload)).toEqual(taskRunCompleteMessages[messageKey])

    if (expectedTaskRunId) {
      // If the message is associated with a task run ID check it has been persisted.
      expect(result.recordset[0].task_run_id).toBe(expectedTaskRunId)
    } else {
      // If the message is not associated with a task run ID check a null value has been persisted.
      expect(result.recordset[0].task_run_id).toBeNull()
    }

    expect(result.recordset[0].description).toBe(expectedErrorDescription)

    await checkAmountOfDataImported(0)
  }

  async function processMessageAndCheckExceptionIsThrown (messageKey, mockErrorResponse) {
    axios.mockRejectedValue(mockErrorResponse)
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
      insert into
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName}
          (workflow_id, filter_id, approved, forecast)
        values
          ('testWorkflow', 'testFilter', 0, 0)
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
