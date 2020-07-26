const axios = require('axios')
const moment = require('moment')
const sql = require('mssql')
const messageFunction = require('../../../ProcessFewsEventCode/index')
const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')

module.exports = function (context, pool, taskRunCompleteMessages) {
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  const processMessage = async function (messageKey, mockResponse) {
    if (mockResponse) {
      axios.get.mockReturnValueOnce(mockResponse)
    } else {
      // Ensure the mocked PI Server is online.
      axios.get.mockReturnValueOnce({
        data: {
          key: 'Filter data'
        }
      })
    }
    await messageFunction(context, taskRunCompleteMessages[messageKey])
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

  this.processMessageAndCheckDataIsCreated = async function (messageKey, expectedData) {
    await processMessage(messageKey)
    const messageDescription = taskRunCompleteMessages[messageKey].input.description
    const messageDescriptionIndex = messageDescription.match(/Task\s+run/) ? 2 : 1
    const expectedTaskRunStartTime = moment(new Date(`${taskRunCompleteMessages['commonMessageData'].startTime} UTC`))
    const expectedTaskRunCompletionTime = moment(new Date(`${taskRunCompleteMessages['commonMessageData'].completionTime} UTC`))
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

      for (const outgoingPlotId of outgoingPlotIds) {
        expect(expectedData.outgoingPlotIds).toContainEqual(outgoingPlotId)
      }

      for (const outgoingFilterId of outgoingFilterIds) {
        expect(expectedData.outgoingFilterIds).toContainEqual(outgoingFilterId)
      }
    } else {
      throw new Error('Expected one TIMESERIES_HEADER record')
    }
  }

  this.processMessageAndCheckNoDataIsCreated = async function (messageKey, expectedNumberOfTimeseriesHeaderRecords, expectedNumberOfOutgoingMessages) {
    await processMessage(messageKey)
    await checkTimeseriesHeaderAndNumberOfOutgoingMessagesCreated(
      expectedNumberOfTimeseriesHeaderRecords || 0, expectedNumberOfOutgoingMessages || 0
    )
  }

  this.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated = async function (messageKey, expectedErrorDescription) {
    await processMessage(messageKey)
    const expectedTaskRunId = taskRunCompleteMessages[messageKey].input ? taskRunCompleteMessages[messageKey].input.source : null
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
    expect(JSON.parse(result.recordset[0].payload)).toEqual(taskRunCompleteMessages[messageKey])

    if (expectedTaskRunId) {
      // If the message is associated with a task run ID check it has been persisted.
      expect(result.recordset[0].task_run_id).toBe(expectedTaskRunId)
    } else {
      // If the message is not associated with a task run ID check a null value has been persisted.
      expect(result.recordset[0].task_run_id).toBeNull()
    }

    expect(result.recordset[0].description).toBe(expectedErrorDescription)
    await checkTimeseriesHeaderAndNumberOfOutgoingMessagesCreated(0, 0)
  }

  this.processMessageAndCheckExceptionIsThrown = async function (messageKey, mockErrorResponse) {
    axios.get.mockRejectedValue(mockErrorResponse)
    await expect(messageFunction(context, taskRunCompleteMessages[messageKey])).rejects.toThrow(mockErrorResponse)
  }

  this.lockWorkflowTableAndCheckMessageCannotBeProcessed = async function (workflow, messageKey, mockResponse) {
    const config = {
      context: context,
      message: taskRunCompleteMessages[messageKey],
      mockResponse: mockResponse,
      processMessageFunction: messageFunction,
      workflow: workflow
    }
    await commonTimeseriesTestUtils.lockWorkflowTableAndCheckMessageCannotBeProcessed(config)
  }
}
