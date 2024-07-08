const axios = require('axios')
const moment = require('moment')
const sql = require('mssql')
const { getEnvironmentVariableAsAbsoluteInteger } = require('../../../Shared/utils')
const messageFunction = require('../../../ProcessFewsEventCode/index')
const CommonTimeseriesTestUtils = require('../shared/common-timeseries-test-utils')
const TASK_RUN_COMPLETION_MESSAGE_DATE_AND_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'

jest.mock('@azure/service-bus')
jest.mock('axios')

module.exports = function (context, pool, taskRunCompleteMessages) {
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  const processMessage = async function (messageKey, sendMessageAsString, axiosMockResponse) {
    if (axiosMockResponse) {
      axios.head.mockReturnValueOnce(axiosMockResponse)
    } else {
      // Ensure the mocked PI Server is online.
      axios.head.mockReturnValueOnce({
        status: 200
      })
    }
    const message = sendMessageAsString && typeof taskRunCompleteMessages[messageKey] !== 'string'
      ? JSON.stringify((taskRunCompleteMessages[messageKey]))
      : taskRunCompleteMessages[messageKey]

    if (message?.taskRunTimesMillisAdjustmentToRelectTimeOfTest) {
      const nowUtc = moment().utc()

      const adjustedTaskRunStartTime =
        moment(nowUtc).subtract(message.taskRunTimesMillisAdjustmentToRelectTimeOfTest, 'milliseconds').format(TASK_RUN_COMPLETION_MESSAGE_DATE_AND_TIME_FORMAT)

      const adjustedTaskRunCompletionTime =
        moment(nowUtc).add(message.taskRunTimesMillisAdjustmentToRelectTimeOfTest, 'milliseconds').format(TASK_RUN_COMPLETION_MESSAGE_DATE_AND_TIME_FORMAT)

      message.input.description =
        message.input.description.replace(`Start time: ${taskRunCompleteMessages.commonMessageData.startTime}`, `Start time: ${adjustedTaskRunStartTime}`)

      message.input.description =
        message.input.description.replace(`End time: ${taskRunCompleteMessages.commonMessageData.completionTime}`, `End time: ${adjustedTaskRunCompletionTime}`)

      message.input.startTime = adjustedTaskRunStartTime
      message.input.endTime = adjustedTaskRunCompletionTime
    }
    await messageFunction(context, message)
    return message
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

  this.processMessageAndCheckDataIsCreated = async function (messageKey, expectedData, sendMessageAsString, axiosMockResponse) {
    const outgoingMessageFilterDelay =
      getEnvironmentVariableAsAbsoluteInteger(process.env.WAIT_FOR_TASK_RUN_FILTER_DATA_AVAILABILITY_MILLIS) || 5000

    const outgoingMessagePlotDelay =
      getEnvironmentVariableAsAbsoluteInteger(process.env.WAIT_FOR_TASK_RUN_PLOT_DATA_AVAILABILITY_MILLIS) || 15000

    const { azureServiceBus, sendMessages, serviceBusClientClose, serviceBusSenderClose } =
      commonTimeseriesTestUtils.mockAzureServiceBusClient()

    const serviceBusClientCallsConfig = {
      azureServiceBus,
      sendMessages,
      serviceBusClientClose,
      serviceBusSenderClose,
      messageSchedulingExpected: expectedData.scheduledMessaging
    }

    azureServiceBus.ServiceBusClient = jest.fn().mockImplementation(connectionString => {
      return {
        close: serviceBusClientClose,
        createSender: jest.fn().mockImplementation(destinationName => {
          return {
            close: serviceBusSenderClose,
            sendMessages
          }
        })
      }
    })

    const message = await processMessage(messageKey, sendMessageAsString, axiosMockResponse)
    const messageDescription = taskRunCompleteMessages[messageKey].input.description
    const messageDescriptionIndex = messageDescription.match(/Task\s+run/) ? 2 : 1

    const expectedTaskRunStartTime =
      moment(new Date(`${message?.input?.startTime ?? taskRunCompleteMessages.commonMessageData.startTime} UTC`))
    const expectedTaskRunCompletionTime =
      moment(new Date(`${message?.input?.endTime ?? taskRunCompleteMessages.commonMessageData.completionTime} UTC`))

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

      // Check for correct outgoing message processing.
      // - If outgoing messages are scheduled they should have been published to fews-import-queue manually.
      // - If outgoing messages are not scheduled they should be on the context binding for fews-import-queue.
      commonTimeseriesTestUtils.checkServiceBusClientCalls(serviceBusClientCallsConfig)

      const messageSource =
        expectedData.scheduledMessaging ? sendMessages.mock.calls[0][0] : context.bindings.importFromFews

      const outgoingPlotIdFunction =
        expectedData.scheduledMessaging ? message => message.body.plotId : message => message.plotId

      const outgoingFilterIdFunction =
        expectedData.scheduledMessaging ? message => message.body.filterId : message => message.filterId

      const outgoingPlotIds = messageSource.filter(outgoingPlotIdFunction).map(outgoingPlotIdFunction)

      const outgoingFilterIds = messageSource.filter(outgoingFilterIdFunction).map(outgoingFilterIdFunction)

      for (const outgoingMessage of messageSource) {
        const messageBody = expectedData.scheduledMessaging ? outgoingMessage.body : outgoingMessage
        expect(messageBody.taskRunId).toBe(expectedTaskRunId)
      }

      expect(outgoingPlotIds.length).toBe((expectedData.outgoingPlotIds || []).length)

      for (const expectedOutgoingPlotId of expectedData.outgoingPlotIds || []) {
        expect(outgoingPlotIds).toContainEqual(expectedOutgoingPlotId)
      }

      expect(outgoingFilterIds.length).toBe((expectedData.outgoingFilterIds || []).length)

      for (const expectedOutgoingFilterId of expectedData.outgoingFilterIds || []) {
        expect(outgoingFilterIds).toContainEqual(expectedOutgoingFilterId)
      }

      if (expectedData.scheduledMessaging) {
        // As scheduled messages are published manually the context binding for fews-import-queue
        // should be an empty array.
        expect(context.bindings.importFromFews.length).toBe(0)

        // Check the scheduling of outgoing messages.
        for (const outgoingMessage of messageSource) {
          const outgoingMessageDelayMillis =
            outgoingMessage.body.filterId ? outgoingMessageFilterDelay : outgoingMessagePlotDelay

          commonTimeseriesTestUtils.checkMessageScheduling(message, outgoingMessage, taskRunCompletionTime, outgoingMessageDelayMillis)
        }
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

  this.processMessageAndCheckMessageIsSentForReplay = async function (messageKey, sendMessageAsString, axiosMockResponse) {
    const message = taskRunCompleteMessages[messageKey]

    const messageReplayDelay =
      getEnvironmentVariableAsAbsoluteInteger(process.env.CHECK_FOR_TASK_RUN_DATA_AVAILABILITY_DELAY_MILLIS) || 2000

    const taskRunCompletionTime = taskRunCompleteMessages.commonMessageData.completionTimem

    const { azureServiceBus, sendMessages, serviceBusClientClose, serviceBusSenderClose } =
      commonTimeseriesTestUtils.mockAzureServiceBusClient()

    const serviceBusClientCallsConfig = {
      azureServiceBus,
      sendMessages,
      serviceBusClientClose,
      serviceBusSenderClose,
      messageSchedulingExpected: true
    }

    await processMessage(messageKey, sendMessageAsString, axiosMockResponse)
    await checkTimeseriesHeaderAndNumberOfOutgoingMessagesCreated(0, 0)
    commonTimeseriesTestUtils.checkServiceBusClientCalls(serviceBusClientCallsConfig)
    commonTimeseriesTestUtils.checkMessageScheduling(message, sendMessages.mock.calls[0][0], taskRunCompletionTime, messageReplayDelay)
  }

  this.processMessageCheckStagingExceptionIsCreatedAndNoDataIsCreated = async function (messageKey, expectedErrorDescription, axiosMockResponse) {
    if (axiosMockResponse) {
      await processMessage(messageKey, false, axiosMockResponse)
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

  this.processMessageAndCheckExceptionIsThrown = async function (messageKey, mockError, axiosMockResponse) {
    // If there is no mock response to return, ensure the mocked PI Server call responds by
    // rejecting a promise using mockError.
    if (!axiosMockResponse) {
      axios.head.mockRejectedValue(mockError)
    }

    if (!axiosMockResponse) {
      // If there is no mock response to return, ensure the mocked PI Server call responds by
      // rejecting a promise using mockError.
      await expect(messageFunction(context, taskRunCompleteMessages[messageKey])).rejects.toThrow(mockError)
    } else {
      // If there is a mock response to return (such as when the handling of a HTTP 206 response
      // code indicating incomplete PI Server indexing is being tested), call processMessages
      // to ensure the PI Server response is mocked correctly. The mocked PI Server response
      // should cause the rejection of a promise using mockError (in the case of a HTTP 206
      // response code, the error causes message replay to be attempted).
      await expect(processMessage(messageKey, false, axiosMockResponse)).rejects.toThrow(mockError)
    }
  }

  this.lockWorkflowTableAndCheckMessageCannotBeProcessed = async function (workflow, messageKey, axiosMockResponse) {
    const config = {
      message: taskRunCompleteMessages[messageKey],
      processMessageFunction: messageFunction,
      context,
      axiosMockResponse,
      workflow
    }
    await commonTimeseriesTestUtils.lockWorkflowTableAndCheckMessageCannotBeProcessed(config)
  }
}
