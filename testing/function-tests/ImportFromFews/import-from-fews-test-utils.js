const { objectToStream } = require('../shared/utils')
const axios = require('axios')
const sql = require('mssql')
const messageFunction = require('../../../ImportFromFews/index')
const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')

module.exports = function (context, pool, importFromFewsMessages, checkImportedDataFunction) {
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

  const checkAmountOfDataImported = async function (taskRunId, expectedNumberOfRecords) {
    const request = new sql.Request(pool)
    await request.input('taskRunId', sql.VarChar, taskRunId)
    const result = await request.query(`
    select
      count(t.id) 
    as 
      number
    from
     fff_staging.timeseries_header th,
     fff_staging.timeseries t
    where
      th.task_run_id = @taskRunId and
      th.id = t.timeseries_header_id
    `)
    expect(result.recordset[0].number).toBe(expectedNumberOfRecords)
  }

  this.processMessagesAndCheckImportedData = async function (config) {
    await processMessages(config.messageKey, config.mockResponses)
    await checkImportedDataFunction(config, context, pool)
    if (config.expectedNumberOfImportedRecords > 0 || (config.mockResponses && config.mockResponses.length)) {
      const taskRunId = importFromFewsMessages[config.messageKey][0].taskRunId
      await checkAmountOfDataImported(taskRunId, config.expectedNumberOfImportedRecords || config.mockResponses.length)
    }
  }

  this.processMessagesAndCheckNoDataIsImported = async function (messageKey, expectedNumberOfRecords) {
    await processMessages(messageKey)
    const taskRunId = importFromFewsMessages[messageKey][0].taskRunId
    await checkAmountOfDataImported(taskRunId, expectedNumberOfRecords || 0)
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
    const taskRunId = importFromFewsMessages[messageKey][0].taskRunId
    await checkAmountOfDataImported(taskRunId, 0)
  }

  this.processMessagesCheckTimeseriesStagingExceptionIsCreatedAndNoDataIsImported = async function (messageKey, mockResponses, expectedErrorDetails, expectedNumberOfRecords) {
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

    const taskRunId = importFromFewsMessages[messageKey][0].taskRunId
    await checkAmountOfDataImported(taskRunId, expectedNumberOfRecords || 0)
  }

  this.processMessagesAndCheckExceptionIsThrown = async function (messageKey, mockErrorResponse) {
    axios.mockRejectedValue(mockErrorResponse)
    for (const message of importFromFewsMessages[messageKey]) {
      await expect(messageFunction(context, message)).rejects.toThrow(mockErrorResponse)
    }
  }

  this.lockWorkflowTableAndCheckMessagesCannotBeProcessed = async function (workflow, messageKey, mockResponse) {
    const config = {
      context: context,
      message: importFromFewsMessages[messageKey][0],
      mockResponse: mockResponse,
      processMessageFunction: messageFunction,
      workflow: workflow
    }
    await commonTimeseriesTestUtils.lockWorkflowTableAndCheckMessageCannotBeProcessed(config)
  }
}
