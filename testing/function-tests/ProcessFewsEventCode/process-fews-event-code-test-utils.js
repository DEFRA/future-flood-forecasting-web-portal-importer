const moment = require('moment')
const sql = require('mssql')
const messageFunction = require('../../../ProcessFewsEventCode/index')

const COASTAL_DISPLAY_GROUP_WORKFLOW_LOCK_TIMEOUT_QUERY = `
  insert into 
    fff_staging.coastal_display_group_workflow
      (workflow_id, plot_id, location_ids) 
    values 
      ('dummyWorkflow', 'dummyPlot', 'dummyLocation')
`
const FLUVIAL_DISPLAY_GROUP_WORKFLOW_LOCK_TIMEOUT_QUERY = `
  insert into 
    fff_staging.fluvial_display_group_workflow
      (workflow_id, plot_id, location_ids) 
    values 
      ('dummyWorkflow', 'dummyPlot', 'dummyLocation')
`
const NON_DISPLAY_GROUP_WORKFLOW_LOCK_TIMEOUT_QUERY = `
  insert into
    fff_staging.non_display_group_workflow
      (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
    values
      ('testWorkflow', 'testFilter', 0, 0, 0, 'external_historical')
 `
const IGNORED_WORKFLOW_LOCK_TIMEOUT_QUERY = `
  insert into
    fff_staging.ignored_workflow
      (workflow_id)
  values 
    ('dummyWorkflow')
`
const lockTimeoutData = {
  'coastalDisplayGroupWorkflow': {
    query: COASTAL_DISPLAY_GROUP_WORKFLOW_LOCK_TIMEOUT_QUERY,
    tableName: 'coastal_display_group_workflow'
  },
  'fluvialDisplayGroupWorkflow': {
    query: FLUVIAL_DISPLAY_GROUP_WORKFLOW_LOCK_TIMEOUT_QUERY,
    tableName: 'coastal_display_group_workflow'
  },
  'nonDisplayGroupWorkflow': {
    query: NON_DISPLAY_GROUP_WORKFLOW_LOCK_TIMEOUT_QUERY,
    tableName: 'coastal_display_group_workflow'
  },
  'ignoredWorkflow': {
    query: IGNORED_WORKFLOW_LOCK_TIMEOUT_QUERY,
    tableName: 'coastal_display_group_workflow'
  }
}

module.exports = function (context, pool, taskRunCompleteMessages) {
  const processMessage = async function (messageKey) {
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

      for (const outgoingPlotMessage of context.bindings.importFromFews) {
        expect(outgoingPlotMessage.taskRunId).toBe(expectedTaskRunId)
      }

      for (const expectedOutgoingPlotId of outgoingPlotIds) {
        expect(outgoingPlotIds).toContainEqual(expectedOutgoingPlotId)
      }

      for (const expectedOutgoingFilterId of outgoingFilterIds) {
        expect(outgoingFilterIds).toContainEqual(expectedOutgoingFilterId)
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

  this.lockDisplayGroupTableAndCheckMessageCannotBeProcessed = async function (workflow, messageKey, mockResponse) {
    let transaction
    try {
      // Lock the timeseries table and then try and process the message.
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(lockTimeoutData[workflow].query)
      await expect(processMessage(messageKey, [mockResponse])).rejects.toBeTimeoutError(lockTimeoutData[workflow].tableName)
    } finally {
      if (transaction._aborted) {
        context.log.warn('The transaction has been aborted.')
      } else {
        await transaction.rollback()
        context.log.warn('The transaction has been rolled back.')
      }
    }
  }
}
