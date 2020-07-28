const moment = require('moment')
const sql = require('mssql')

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
        convert(bit, case
          when t.fews_parameters like '&plotId=%' then 1
          else 0
          end
        ) as is_plot
      from
        fff_staging.timeseries_header th,
        fff_staging.timeseries t
      where
        th.id = t.timeseries_header_id
    `)

    expect(result.recordset.length).toBe(config.mockResponses.length)

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
      receivedFewsData.push(JSON.parse(result.recordset[index].fews_data))
      receivedPrimaryKeys.push(result.recordset[index].id)

      // Check that plot timeseries data has been persisted correctly (filter timeseries data is checked in other unit tests).
      if (result.recordset[index].is_plot) {
        const taskRunCompletionTime = moment(result.recordset[index].task_completion_time)

        // Check that the persisted values for the forecast start time and end time are based within expected range of
        // the task run completion time taking into acccount that the default values can be overridden by environment variables.
        const startTimeDisplayGroupOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 14
        const endTimeOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120
        const expectedStartTime = moment(taskRunCompletionTime).subtract(startTimeDisplayGroupOffsetHours, 'hours').toISOString().substring(0, 19)
        const expectedEndTime = moment(taskRunCompletionTime).add(endTimeOffsetHours, 'hours').toISOString().substring(0, 19)
        expect(result.recordset[index].fews_parameters).toContain(`&startTime=${expectedStartTime}Z`)
        expect(result.recordset[index].fews_parameters).toContain(`&endTime=${expectedEndTime}Z`)
      }
    }

    for (const mockResponse of config.mockResponses) {
      expect(receivedFewsData).toContainEqual(mockResponse.data)
    }

    // The following check is for when there is an output binding named 'stagedTimeseries' active.
    if (process.env.IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED === true) {
      for (const stagedTimeseries of context.bindings.stagedTimeseries) {
        expect(receivedPrimaryKeys).toContainEqual(stagedTimeseries.id)
      }
    }
  }
}