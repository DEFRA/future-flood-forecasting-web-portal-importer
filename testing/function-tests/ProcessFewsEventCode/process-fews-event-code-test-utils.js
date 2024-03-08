const axios = require('axios')
const moment = require('moment')
const sql = require('mssql')
const messageFunction = require('../../../ProcessFewsEventCode/index')
const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')
jest.mock('axios')
module.exports = function (context, pool, taskRunCompleteMessages) {
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  const processMessage = async function (messageKey, sendMessageAsString, mockResponse) {
    if (mockResponse) {
      axios.get.mockReturnValueOnce(mockResponse)
    } else {
      // Ensure the mocked PI Server is online.
      axios.get.mockReturnValueOnce({
        status: 200,
        data: {
          key: 'Filter data'
        }
      })
    }
    const message = sendMessageAsString ? JSON.stringify(taskRunCompleteMessages[messageKey]) : taskRunCompleteMessages[messageKey]
    await messageFunction(context, message)
  }

  const checkTimeseriesHeaderAndNumberOfOutgoingMessagesCreated = async function (expectedNumberOfTimeseriesHeaderRecords, expectedNumberOfOutgoingMessages) {
    const request = new sql.Request(pool)
    const result = await request.query(`
    select
      count(id) 
    as 
      number
    from
     fff_staging.timeseries_header
    `)
    expect(result.recordset[0].number).toBe(expectedNumberOfTimeseriesHeaderRecords)
    expect(context.bindings.importFromFews.length).toBe(expectedNumberOfOutgoingMessages)
  }

  const checkExpectedActiveTimeseriesStagingExceptionsForTaskRun = async function (taskRunId, expectedData) {
    const expectedTimeseriesStagingExceptionsForTaskRun = expectedData.remainingTimeseriesStagingExceptions || []
    const request = new sql.Request(pool)
    await request.input('taskRunId', sql.NVarChar, taskRunId)
    const result = await request.query(`
      select
        tse.source_id,
        tse.source_type
      from
        fff_staging.v_active_timeseries_staging_exception tse,
        fff_staging.timeseries_header th
      where
        th.task_run_id = @taskRunId and
        th.id = tse.timeseries_header_id
      order by
        tse.source_id
    `)
    expect(result.recordset.length).toBe(expectedTimeseriesStagingExceptionsForTaskRun.length)
    for (const index in expectedTimeseriesStagingExceptionsForTaskRun) {
      expect(result.recordset[index].source_id).toBe(expectedTimeseriesStagingExceptionsForTaskRun[index].sourceId)
      expect(result.recordset[index].source_type).toBe(expectedTimeseriesStagingExceptionsForTaskRun[index].sourceType)
    }
  }

  const checkExpectedActiveStagingExceptionsForTaskRun = async function (taskRunId, expectedNumberOfStagingExceptions) {
    const request = new sql.Request(pool)
    await request.input('taskRunId', sql.NVarChar, taskRunId)
    const result = await request.query(`
      select 
        count(*) as count
      from
        fff_staging.v_active_staging_exception tse
    `)
    expect(result.recordset[0].count).toBe(expectedNumberOfStagingExceptions)
  }

  this.processMessageAndCheckDataIsCreated = async function (messageKey, expectedData, sendMessageAsString, mockResponse) {
    await processMessage(messageKey, sendMessageAsString, mockResponse)
    const messageDescription = taskRunCompleteMessages[messageKey].input.description
    const messageDescriptionIndex = messageDescription.match(/Task\s+run/) ? 2 : 1
    const expectedTaskRunStartTime = moment(new Date(`${taskRunCompleteMessages.commonMessageData.startTime} UTC`))
    const expectedTaskRunCompletionTime = moment(new Date(`${taskRunCompleteMessages.commonMessageData.completionTime} UTC`))
    const expectedTaskRunId = taskRunCompleteMessages[messageKey].input.source
    const expectedWorkflowId = taskRunCompleteMessages[messageKey].input.description.split(/\s+/)[messageDescriptionIndex]
    const request = new sql.Request(pool)
    const result = await request.query(`
    select
      th.workflow_id,
      th.task_run_id,
      th.task_start_time,
      th.task_completion_time,
      th.forecast,
      th.approved,
      th.message
    from
      fff_staging.timeseries_header th
    `)

    // Check that a TIMESERIES_HEADER record has been persisted correctly
    if (result.recordset.length === 1) {
      const taskRunStartTime = moment(result.recordset[0].task_start_time)
      expect(taskRunStartTime.toISOString()).toBe(expectedTaskRunStartTime.toISOString())
      const taskRunCompletionTime = moment(result.recordset[0].task_completion_time)
      expect(taskRunCompletionTime.toISOString()).toBe(expectedTaskRunCompletionTime.toISOString())
      expect(result.recordset[0].task_run_id).toBe(expectedTaskRunId)
      expect(result.recordset[0].workflow_id).toBe(expectedWorkflowId)
      expect(result.recordset[0].forecast).toBe(expectedData.forecast)
      expect(result.recordset[0].approved).toBe(expectedData.approved)

      // Check the incoming message has been captured correctly.
      expect(JSON.parse(result.recordset[0].message)).toEqual(taskRunCompleteMessages[messageKey])

      const outgoingPlotIds =
        context.bindings.importFromFews.filter(message => message.plotId)
          .map(message => message.plotId)

      const outgoingFilterIds =
        context.bindings.importFromFews.filter(message => message.filterId)
          .map(message => message.filterId)

      for (const outgoingMessage of context.bindings.importFromFews) {
        expect(outgoingMessage.taskRunId).toBe(expectedTaskRunId)
      }

      expect(outgoingPlotIds.length).toBe((expectedData.outgoingPlotIds || []).length)

      for (const expectedOutgoingPlotId of expectedData.outgoingPlotIds || []) {
        expect(outgoingPlotIds).toContainEqual(expectedOutgoingPlotId)
      }

      expect(outgoingFilterIds.length).toBe((expectedData.outgoingFilterIds || []).length)

      for (const expectedOutgoingFilterId of expectedData.outgoingFilterIds || []) {
        expect(outgoingFilterIds).toContainEqual(expectedOutgoingFilterId)
      }

      const stagingExceptionConfig = {
        sourceFunction: 'P',
        taskRunId: expectedTaskRunId,
        expectedNumberOfStagingExceptions: expectedData.expectedNumberOfStagingExceptions || 0
      }

      await commonTimeseriesTestUtils.checkNumberOfActiveStagingExceptionsForSourceFunctionOfWorkflow(stagingExceptionConfig)
      await checkExpectedActiveTimeseriesStagingExceptionsForTaskRun(expectedTaskRunId, expectedData)
    } else {
      throw new Error('Expected one TIMESERIES_HEADER record')
    }
  }

  this.processMessageAndCheckNoDataIsCreated = async function (messageKey, expectedNumberOfTimeseriesHeaderRecords, expectedNumberOfOutgoingMessages, expectedNumberOfStagingExceptions) {
    await processMessage(messageKey)
    await checkTimeseriesHeaderAndNumberOfOutgoingMessagesCreated(
      expectedNumberOfTimeseriesHeaderRecords || 0, expectedNumberOfOutgoingMessages || 0
    )
    let taskRunId
    if (taskRunCompleteMessages[messageKey] && taskRunCompleteMessages[messageKey].input) {
      taskRunId = taskRunCompleteMessages[messageKey].input.source
    } else {
      taskRunId = null
    }
    await checkExpectedActiveStagingExceptionsForTaskRun(taskRunId, expectedNumberOfStagingExceptions || 0)
  }

  this.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated = async function (messageKey, expectedErrorDescription, mockResponse) {
    if (mockResponse) {
      await processMessage(messageKey, taskRunCompleteMessages[messageKey], mockResponse)
    } else {
      await processMessage(messageKey)
    }
    const expectedTaskRunId = taskRunCompleteMessages[messageKey].input ? taskRunCompleteMessages[messageKey].input.source : null
    const request = new sql.Request(pool)
    const result = await request.query(`
    select top(1)
      payload,
      task_run_id,
      description,
      source_function
    from
      fff_staging.v_active_staging_exception
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
    expect(result.recordset[0].source_function).toBe('P')
    await checkTimeseriesHeaderAndNumberOfOutgoingMessagesCreated(0, 0)
  }

  this.processMessageAndCheckExceptionIsThrown = async function (messageKey, mockError, mockResponse) {
    // If there is no mock response to return, ensure the mocked PI Server call responds by
    // rejecting a promise using mockError.
    if (!mockResponse) {
      axios.get.mockRejectedValue(mockError)
    }

    if (!mockResponse) {
      // If there is no mock response to return, ensure the mocked PI Server call responds by
      // rejecting a promise using mockError.
      await expect(messageFunction(context, taskRunCompleteMessages[messageKey])).rejects.toThrow(mockError)
    } else {
      // If there is a mock response to return (such as when the handling of a HTTP 206 response
      // code indicating incomplete PI Server indexing is being tested), call processMessages
      // to ensure the PI Server response is mocked correctly. The mocked PI Server response
      // should cause the rejection of a promise using mockError (in the case of a HTTP 206
      // response code, the error causes message replay to be attempted).
      await expect(processMessage(messageKey, mockError, mockResponse)).rejects.toThrow(mockError)
    }
  }

  this.lockWorkflowTableAndCheckMessageCannotBeProcessed = async function (workflow, messageKey, mockResponse) {
    const config = {
      message: taskRunCompleteMessages[messageKey],
      processMessageFunction: messageFunction,
      context,
      mockResponse,
      workflow
    }
    await commonTimeseriesTestUtils.lockWorkflowTableAndCheckMessageCannotBeProcessed(config)
  }
}
