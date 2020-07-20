const { objectToStream } = require('../shared/utils')
const moment = require('moment')
const axios = require('axios')
const sql = require('mssql')
const messageFunction = require('../../../ImportFromFews/index')
const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')

module.exports = function (context, pool, importFromFewsMessages) {
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  const processMessages = async function (messageKey, mockResponses) {
    if (mockResponses) {
      let mock = axios
      for (const mockResponse of mockResponses) {
        const mockResponseWithDataAsStream = Object.assign({}, mockResponse)
        if (mockResponse instanceof Error) {
          mockResponseWithDataAsStream.response.data = await objectToStream(mockResponse.response.data)
          // A mock Error is being cloned so just copy the error message and stack trace manually.
          mockResponseWithDataAsStream.message = mockResponse.message
          mockResponseWithDataAsStream.stack = mockResponse.stack
          mock = axios.mockRejectedValue(mockResponseWithDataAsStream)
        } else {
          mockResponseWithDataAsStream.data = await objectToStream(mockResponse.data)
          mock = mock.mockReturnValueOnce(mockResponseWithDataAsStream)
        }
      }
    }
    for (const message of importFromFewsMessages[messageKey]) {
      await messageFunction(context, message)
    }
  }

  const checkAmountOfDataImported = async function (expectedNumberOfRecords) {
    const request = new sql.Request(pool)
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

  this.processMessagesAndCheckImportedData = async function (messageKey, mockResponses, workflowAlreadyRan) {
    await processMessages(messageKey, mockResponses)

    const receivedFewsData = []
    const receivedPrimaryKeys = []

    let excludeFilterString = ''
    if (workflowAlreadyRan && workflowAlreadyRan.spanFlag === true) {
      excludeFilterString = workflowAlreadyRan.plotIdTargetedQuery
    }

    const request = new sql.Request(pool)
    const result = await request.query(`
    select
      t.id,
      t.fews_parameters,
      th.workflow_id,
      th.task_run_id,
      th.task_completion_time,
      cast(decompress(t.fews_data) as varchar(max)) as fews_data
    from
      fff_staging.timeseries_header th,
      fff_staging.timeseries t
    where
      th.id = t.timeseries_header_id ${excludeFilterString}
    `)

    if (workflowAlreadyRan && workflowAlreadyRan.spanFlag) {
      expect(result.recordset.length).toBe(workflowAlreadyRan.expectedTargetedQueryLength)
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
      const taskRunCompletionTime = moment(result.recordset[index].task_completion_time)

      // Check that the persisted values for the forecast start time and end time are based within expected range of
      // the task run completion time taking into acccount that the default values can be overridden by environment variables.
      const startTimeDisplayGroupOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 14
      const endTimeOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120
      const expectedStartTime = moment(taskRunCompletionTime).subtract(startTimeDisplayGroupOffsetHours, 'hours').toISOString().substring(0, 19)
      const expectedEndTime = moment(taskRunCompletionTime).add(endTimeOffsetHours, 'hours').toISOString().substring(0, 19)
      expect(result.recordset[index].fews_parameters).toContain(`&startTime=${expectedStartTime}Z`)
      expect(result.recordset[index].fews_parameters).toContain(`&endTime=${expectedEndTime}Z`)

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

  this.processMessagesAndCheckNoDataIsImported = async function (messageKey, expectedNumberOfRecords) {
    await processMessages(messageKey)
    await checkAmountOfDataImported(expectedNumberOfRecords || 0)
  }

  this.processMessagesCheckStagingExceptionIsCreatedAndNoDataIsImported = async function (messageKey, expectedErrorDescription) {
    await processMessages(messageKey)
    const expectedTaskRunId = importFromFewsMessages[messageKey][0].taskRunId
    const request = new sql.Request(pool)
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
    expect(JSON.parse(result.recordset[0].payload)).toEqual(importFromFewsMessages[messageKey][0])

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

  this.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported = async function (messageKey, mockResponses, expectedErrorDetails) {
    await processMessages(messageKey, mockResponses)
    const request = new sql.Request(pool)
    const result = await request.query(`
    select top(1)
      source_id,
      source_type,
      csv_error,
      csv_type,
      description
    from
      fff_staging.timeseries_staging_exception
    order by
      exception_time desc
    `)

    // Check the error details have been captured correctly.
    expect(result.recordset[0].source_id).toEqual(expectedErrorDetails.sourceId)
    expect(result.recordset[0].source_type).toEqual(expectedErrorDetails.sourceType)
    expect(result.recordset[0].csv_error).toEqual(expectedErrorDetails.csvError)
    expect(result.recordset[0].csv_type).toEqual(expectedErrorDetails.csvType)
    expect(result.recordset[0].description).toEqual(expectedErrorDetails.description)

    await checkAmountOfDataImported(0)
  }

  this.processMessagesAndCheckExceptionIsThrown = async function (messageKey, mockErrorResponse) {
    axios.mockRejectedValue(mockErrorResponse)
    for (const message of importFromFewsMessages[messageKey]) {
      await expect(messageFunction(context, message)).rejects.toThrow(mockErrorResponse)
    }
  }

  this.lockDisplayGroupTableAndCheckMessagesCannotBeProcessed = async function (workflow, messageKey, mockResponse) {
    const config = {
      context: context,
      message: importFromFewsMessages[messageKey][0],
      mockResponse: mockResponse,
      processMessageFunction: messageFunction,
      workflow: workflow
    }
    await commonTimeseriesTestUtils.lockDisplayGroupTableAndCheckMessageCannotBeProcessed(config)
  }
}
