// const moment = require('moment')
const sql = require('mssql')
const CommonTimeseriesTestUtils = require('./common-timeseries-test-utils')

module.exports = function (pool, messages) {
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  this.beforeAll = async function () {
    await commonTimeseriesTestUtils.beforeAll(pool)
    const request = new sql.Request(pool)
    await request.batch(`
        insert into
          fff_staging.non_display_group_workflow
             (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('Test_Workflow1', 'Test Filter1', 0, 0, 0, 'external_historical'),
          ('Test_Workflow2', 'Test Filter2a', 0, 0, 0, 'external_historical'),
          ('Test_Workflow2', 'Test Filter2b', 0, 0, 0, 'external_historical'),
          ('Test_Workflow3', 'Test Filter3', 1, 0, 0, 'external_historical'),
          ('Test_Workflow4', 'Test Filter4', 1, 0, 0, 'external_historical'),
          ('Span_Workflow', 'SpanFilter', 1, 0, 0, 'external_historical'),
          ('Test_workflowCustomTimes', 'Test FilterCustomTimes', 1, '10', '20', 'external_historical'),
          ('workflow_simulated_forecasting', 'Test Filter SF', 1, 0, 0, 'simulated_forecasting'),
          ('workflow_external_forecasting', 'Test Filter EF', 0, 0, 0, 'external_forecasting'),
          ('workflow_external_historical', 'Test Filter EH', 0, 0, 0, 'external_historical'),
          ('workflow_unknown_timeseries_type', 'Test Filter Unknown Timeseries Type', 0, 0, 0, 'Not specified')
      `)
  }
  this.beforeEach = async function () {
    await commonTimeseriesTestUtils.beforeEach(pool)
  }
  this.afterAll = async function () {
    await commonTimeseriesTestUtils.afterAll(pool)
  }
}
