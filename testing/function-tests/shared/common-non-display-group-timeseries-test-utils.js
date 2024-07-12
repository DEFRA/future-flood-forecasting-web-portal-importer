const sql = require('mssql')
const CommonTimeseriesTestUtils = require('./common-timeseries-test-utils')

module.exports = function (pool, messages) {
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  this.beforeAll = async function () {
    await commonTimeseriesTestUtils.beforeAll()
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
          ('Disaster_Recovery_Workflow', 'Test Filter Disaster Recovery', 1, 0, 0, 'external_historical'),
          ('Span_Workflow', 'SpanFilter', 1, 0, 0, 'external_historical'),
          ('Test_workflowCustomTimes', 'Test FilterCustomTimes', 1, '10', '20', 'external_historical'),
          ('Simulated_Forecasting_Workflow1', 'Test Filter SF', 1, 0, 0, 'simulated_forecasting'),
          ('Simulated_Forecasting_Workflow2', 'Test Filter SF', 1, 0, 0, 'simulated_forecasting'),
          ('External_Forecasting_Workflow1', 'Test Filter EF', 0, 0, 0, 'external_forecasting'),
          ('External_Forecasting_Workflow2', 'Test Filter EF', 0, 0, 0, 'external_forecasting'),
          ('External_Historical_Workflow', 'Test Filter EH', 0, 0, 0, 'external_historical'),
          ('Unknown_Timeseries_Type_Workflow', 'Test Filter Unknown Timeseries Type', 0, 0, 0, 'Not specified'),
          ('Partial_Load_Span_Workflow', 'Test Span Filter 9a', 1, 0, 0, 'external_historical'),
          ('Partial_Load_Span_Workflow', 'Test Span Filter 9b', 1, 0, 0, 'external_historical'),
          ('Custom_Offset_Workflow', 'Custom_Offset_Filter', 0, 10, 20, 'external_historical'),
          ('Custom_Offset_Workflow_Forecast', 'Custom_Offset_Filter_Forecast', 0, 8, 12, 'simulated_forecasting'),
          ('Missing_Event_Workflow', 'Filter_With_Missing_Events', 0, 0, 0, 'external_historical'),
          ('No_Missing_Events_Workflow', 'Filter_Without_Missing_Events', 0, 0, 0, 'external_historical')
      `)
  }
  this.beforeEach = async function () {
    await commonTimeseriesTestUtils.beforeEach()
  }
  this.afterAll = async function () {
    await commonTimeseriesTestUtils.afterAll()
  }
}
