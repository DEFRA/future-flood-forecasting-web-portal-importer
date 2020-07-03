module.exports = describe('Tests for import timeseries display groups', () => {
  const taskRunCompleteMessages = require('../testing/messages/task-run-complete/fluvial-display-group-messages')
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

  describe('Message processing for fluvial display group task run completion', () => {
    beforeAll(async () => {
      await pool.connect()
      await request.batch(`delete from fff_staging.fluvial_display_group_workflow`)
      await request.batch(`delete from fff_staging.non_display_group_workflow`)
      await request.batch(`delete from fff_staging.ignored_workflow`)
      await request.batch(`
        insert into
          fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
        values
          ('Test_Fluvial_Workflow1', 'Test Plot1', 'Test Location1'), ('Test_Fluvial_Workflow2', 'Test Plot2a', 'Test Location2a'), ('Test_Fluvial_Workflow2', 'Test Plot2b', 'Test Location2b')
      `)
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      await request.batch(`delete from fff_staging.timeseries`)
      await request.batch(`delete from fff_staging.timeseries_header`)
      await request.batch(`delete from fff_staging.staging_exception`)
    })
    afterAll(async () => {
      await request.batch(`delete from fff_staging.ignored_workflow`)
      await request.batch(`delete from fff_staging.fluvial_display_group_workflow`)
      await request.batch(`delete from fff_staging.non_display_group_workflow`)
      await request.batch(`delete from fff_staging.timeseries`)
      await request.batch(`delete from fff_staging.timeseries_header`)
      await request.batch(`delete from fff_staging.staging_exception`)
      // Closing the DB connection allows Jest to exit successfully.
      await pool.close()
    })
    it('should import data for a single plot associated with an approved forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await processMessageAndCheckImportedData('singlePlotApprovedForecast', [mockResponse])
    })
    it('should import data for multiple plots associated with an approved forecast', async () => {
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
      await processMessageAndCheckImportedData('multiplePlotApprovedForecast', mockResponses)
    })
    it('should not import data for an unapproved forecast', async () => {
      await processMessageAndCheckNoDataIsImported('unapprovedForecast')
    })
    it('should not import data for an out of date forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await processMessageAndCheckImportedData('singlePlotApprovedForecast', [mockResponse])
      await processMessageAndCheckNoDataIsImported('earlierSinglePlotApprovedForecast', 1)
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
        await processMessageAndCheckImportedData('singlePlotApprovedForecast', [mockResponse])
      } finally {
        process.env = originalEnvironment
      }
    })
    it('should create a staging exception for an unknown workflow', async () => {
      const unknownWorkflow = 'unknownWorkflow'
      const workflowId = taskRunCompleteMessages[unknownWorkflow].input.description.split(/\s+/)[1]
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported(unknownWorkflow, `Missing PI Server input data for ${workflowId}`)
    })
    it('should create a staging exception for an invalid message', async () => {
      await processMessageCheckStagingExceptionIsCreatedAndNoDataIsImported('forecastWithoutApprovalStatus', 'Unable to extract task run Approved status from message')
    })
    it('should throw an exception when the core engine PI server is unavailable', async () => {
      // If the core engine PI server is down messages are eligible for replay a certain number of times so check that
      // an exception is thrown to facilitate this process.
      const mockResponse = new Error('connect ECONNREFUSED mockhost')
      await processMessageAndCheckExceptionIsThrown('singlePlotApprovedForecast', mockResponse)
    })
    it('should create a staging exception when a core engine PI server resource is unavailable', async () => {
      // If a core engine PI server resource is unvailable (HTTP response code 404), messages are probably eligible for replay a certain number of times so
      // check that an exception is thrown to facilitate this process. If misconfiguration has occurred, the maximum number
      // of replays will be reached and the message will be transferred to a dead letter queue for manual intervetion.
      const mockResponse = new Error('Request failed with status code 404')
      await processMessageAndCheckExceptionIsThrown('singlePlotApprovedForecast', mockResponse)
    })
    it('should throw an exception when the fluvial_display_group_workflow table is being refreshed', async () => {
      // If the fluvial_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await lockDisplayGroupTableAndCheckMessageCannotBeProcessed('singlePlotApprovedForecast', mockResponse)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    it('should import data for a single plot associated with an approved forecast', async () => {
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      process.env.IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED = true // in this case the build script would contain function.json with an output binding
      context.bindingDefinitions = [{ direction: 'out', name: 'stagedTimeseries', type: 'servieBus' }]
      await processMessageAndCheckImportedData('singlePlotApprovedForecast', [mockResponse])
    })
  })

  async function processMessage (messageKey, mockResponses) {
    if (mockResponses) {
      let mock = axios
      for (const mockResponse of mockResponses) {
        mock = mock.mockReturnValueOnce({ data: await objectToStream(mockResponse.data) })
      }
    }
    await messageFunction(context, JSON.stringify(taskRunCompleteMessages[messageKey]))
  }

  async function processMessageAndCheckImportedData (messageKey, mockResponses) {
    await processMessage(messageKey, mockResponses)
    const messageDescription = taskRunCompleteMessages[messageKey].input.description
    const messageDescriptionIndex = messageDescription.match(/Task\s+run/) ? 2 : 1
    const expectedTaskRunCompletionTime = moment(new Date(`${taskRunCompleteMessages['commonMessageData'].completionTime} UTC`))
    const expectedTaskRunId = taskRunCompleteMessages[messageKey].input.source
    const expectedWorkflowId = taskRunCompleteMessages[messageKey].input.description.split(/\s+/)[messageDescriptionIndex]
    const receivedFewsData = []
    const receivedPrimaryKeys = []

    const result = await request.query(`
      select
        t.id,
        t.fews_parameters,
        th.workflow_id,
        th.task_run_id,
        th.task_completion_time,
        th.message,
        cast(decompress(t.fews_data) as varchar(max)) as fews_data
      from
        fff_staging.timeseries_header th,
        fff_staging.timeseries t
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
        const taskRunCompletionTime = moment(result.recordset[index].task_completion_time)

        expect(taskRunCompletionTime.toISOString()).toBe(expectedTaskRunCompletionTime.toISOString())
        expect(result.recordset[index].task_run_id).toBe(expectedTaskRunId)
        expect(result.recordset[index].workflow_id).toBe(expectedWorkflowId)

        // Check that the persisted values for the forecast start time and end time are based within expected range of
        // the task run completion time taking into acccount that the default values can be overridden by environment variables.
        const startTimeOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 14
        const endTimeOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120
        const expectedStartTime = moment(taskRunCompletionTime).subtract(startTimeOffsetHours, 'hours')
        const expectedEndTime = moment(taskRunCompletionTime).add(endTimeOffsetHours, 'hours')
        expect(result.recordset[index].fews_parameters).toContain(`&startTime=${expectedStartTime.toISOString().substring(0, 19)}Z`)
        expect(result.recordset[index].fews_parameters).toContain(`&endTime=${expectedEndTime.toISOString().substring(0, 19)}Z`)

        // Check the incoming message has been captured correctly.
        expect(JSON.parse(result.recordset[index].message)).toEqual(taskRunCompleteMessages[messageKey])
      }
      receivedFewsData.push(JSON.parse(result.recordset[index].fews_data))
      receivedPrimaryKeys.push(result.recordset[index].id)
    }

    for (const mockResponse of mockResponses) {
      expect(receivedFewsData).toContainEqual(mockResponse.data)
    }

    // The following check is for when there is an output binding named 'stagedTimeseries' active.
    if (process.env.IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED === true) {
      for (const stagedTimeseries of context.bindings.stagedTimeseries) {
        expect(receivedPrimaryKeys).toContainEqual(stagedTimeseries.id)
      }
    }
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
      fff_staging.timeseries_header th,
      fff_staging.timeseries t
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
      fff_staging.staging_exception
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
    await expect(messageFunction(context, JSON.stringify(taskRunCompleteMessages[messageKey])))
      .rejects.toThrow(mockErrorResponse)
  }

  async function lockDisplayGroupTableAndCheckMessageCannotBeProcessed (messageKey, mockResponse) {
    let transaction
    const tableName = 'fluvial_display_group_workflow'
    try {
      // Lock the timeseries table and then try and process the message.
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(`
      insert into 
        fff_staging.${tableName} (workflow_id, plot_id, location_ids)
      values 
        ('dummyWorkflow', 'dummyPlot', 'dummyLocation')
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
