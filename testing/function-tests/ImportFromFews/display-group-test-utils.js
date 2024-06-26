const moment = require('moment')
const sql = require('mssql')
const { isBoolean } = require('../../../Shared/utils')

module.exports = {
  checkImportedData: async function (config, context, pool) {
    const receivedFewsData = []
    const receivedPrimaryKeys = []

    const request = new sql.Request(pool)
    const result = await request.query(`
      select
        t.id,
        t.fews_parameters,
        th.workflow_id,
        th.task_run_id,
        th.task_completion_time,
        cast(decompress(t.fews_data) as varchar(max)) as fews_data,
        case
          when t.fews_parameters like '&plotId=%' then substring(fews_parameters, charindex('&plotId=', fews_parameters) + 8,  charindex('&', fews_parameters, 7) - (charindex('&plotId=', fews_parameters) + 8))
          when t.fews_parameters like '&filterId=%' then substring(fews_parameters, charindex('&filterId=', fews_parameters) + 10,  charindex('&', fews_parameters, 9) - (charindex('&filterId=', fews_parameters) + 10))
          end
        as source_id,
        case
          when t.fews_parameters like '&plotId=%' then 'P'
          when t.fews_parameters like '&filterId=%' then 'F'
          end
        as source_type
      from
        fff_staging.timeseries_header th,
        fff_staging.timeseries t
      where
        th.id = t.timeseries_header_id
      order by
        t.import_time desc
    `)

    expect(result.recordset.length).toBe(config.expectedNumberOfImportedRecords || config.mockResponses.length)

    // Database interaction is asynchronous so the order in which records are written
    // cannot be guaranteed.
    // To check if records have been persisted correctly, copy the timeseries data
    // retrieved from the database to an array and then check that the array contains
    // each expected mock timeseries.
    // To check if messages containing the primary keys of the timeseries records will be
    // sent to a queue for reporting and visualisation purposes, copy the primary
    // keys retrieved from the database to an array and check that the ouput binding for
    // staged timeseries contains each expected primary key.
    for (const index in result.recordset) {
      receivedFewsData.push(JSON.parse(result.recordset[index].fews_data))
      receivedPrimaryKeys.push(result.recordset[index].id)

      // Check that plot timeseries data has been persisted correctly (filter timeseries data is checked in other unit tests).
      if (result.recordset[index].source_type === 'P') {
        const taskRunCompletionTime = moment(result.recordset[index].task_completion_time)

        let startTimeDisplayGroupOffsetHours
        let endTimeDisplayGroupOffsetHours
        // Check that the persisted values for the forecast start time and end time are based within expected range of
        // the task run completion time taking into acccount that the default values can be overridden by environment variables.
        if (config.spanWorkflowId) {
          const offsetData = await getWorkflowOffsetData(context, pool, config.spanWorkflowId)
          if (offsetData && offsetData.startTimeOffset && offsetData.startTimeOffset !== 0) {
            startTimeDisplayGroupOffsetHours = offsetData.startTimeOffset
          } else {
            startTimeDisplayGroupOffsetHours = process.env.FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS ? parseInt(process.env.FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS) : 24
          }
          if (offsetData && offsetData.endTimeOffset && offsetData.endTimeOffset !== 0) {
            endTimeDisplayGroupOffsetHours = offsetData.endTimeOffset
          } else {
            endTimeDisplayGroupOffsetHours = 0
          }
        } else {
          startTimeDisplayGroupOffsetHours = process.env.FEWS_DISPLAY_GROUP_START_TIME_OFFSET_HOURS ? parseInt(process.env.FEWS_DISPLAY_GROUP_START_TIME_OFFSET_HOURS) : 14
          endTimeDisplayGroupOffsetHours = process.env.FEWS_DISPLAY_GROUP_END_TIME_OFFSET_HOURS ? parseInt(process.env.FEWS_DISPLAY_GROUP_END_TIME_OFFSET_HOURS) : 120
        }

        const expectedStartTime = moment(taskRunCompletionTime).subtract(startTimeDisplayGroupOffsetHours, 'hours').toISOString().substring(0, 19)
        const expectedEndTime = moment(taskRunCompletionTime).add(endTimeDisplayGroupOffsetHours, 'hours').toISOString().substring(0, 19)
        expect(result.recordset[index].fews_parameters).toContain(`&startTime=${expectedStartTime}Z`)
        expect(result.recordset[index].fews_parameters).toContain(`&endTime=${expectedEndTime}Z`)

        if (config.expectedLocationData && result.recordset[index].source_id === config.expectedLocationData[index].plotId) {
          // Check that the persisted FEWS parameters contain expected locations.
          for (const location of config.expectedLocationData[index].includedLocations) {
            expect(result.recordset[index].fews_parameters).toContain(`&locationIds=${location}`)
          }

          for (const location of config.expectedLocationData[index].excludedLocations) {
            expect(result.recordset[index].fews_parameters).not.toContain(`&locationIds=${location}`)
          }
        }
      }
    }

    const nonErrorMockResponses =
      config.expectedFewsData || config.mockResponses.filter(mockResponse => !(mockResponse instanceof Error))

    for (const mockResponse of nonErrorMockResponses) {
      expect(receivedFewsData).toContainEqual(mockResponse.data)
    }

    // The following check is for when there is an output binding named 'stagedTimeseries' active.
    if (isBoolean(process.env.IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED) &&
        JSON.parse(process.env.IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED)) {
      for (const stagedTimeseries of context.bindings.stagedTimeseries) {
        expect(receivedPrimaryKeys).toContainEqual(stagedTimeseries.id)
      }
    }
  }
}

async function getWorkflowOffsetData (context, pool, workflowId) {
  const request = new sql.Request(pool)
  await request.input('workflowId', sql.NVarChar, workflowId)
  const result = await request.query(`
    select distinct
      start_time_offset_hours,
      end_time_offset_hours
    from
      fff_staging.non_display_group_workflow
    with
      (tablock holdlock)
    where
      workflow_id = @workflowId
  `)

  let offsetData

  if (result && result.recordset && result.recordset[0] && result.recordset.length === 1) {
    offsetData = {
      startTimeOffset: result.recordset[0].start_time_offset_hours,
      endTimeOffset: result.recordset[0].end_time_offset_hours
    }
  } else {
    if (result && result.recordset && result.recordset[0] && result.recordset.length > 1) {
      context.log('Multiple custom offsets have been found.')
    } else {
      context.log('No offsets found.')
    } offsetData = null
  }
  return offsetData
}
