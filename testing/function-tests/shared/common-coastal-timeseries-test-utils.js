const sql = require('mssql')
const CommonTimeseriesTestUtils = require('./common-timeseries-test-utils')

module.exports = function (pool, messages) {
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  this.beforeAll = async function () {
    await commonTimeseriesTestUtils.beforeAll()
    const request = new sql.Request(pool)
    await request.batch(`
      insert into
        fff_staging.coastal_display_group_workflow (workflow_id, plot_id, location_ids)
      values
        ('Test_Coastal_Workflow', 'Test Coastal Plot', 'Test Coastal Location'),
        ('Test_Coastal_Workflow1', 'Test Coastal Plot 1', 'Test Coastal Location 1'),
        ('Test_Coastal_Workflow2', 'Test Coastal Plot 2a', 'Test Coastal Location 2a-1;Test Coastal Location 2a-2;Test Coastal Location 2a-3'),
        ('Test_Coastal_Workflow2', 'Test Coastal Plot 2b', 'Test Coastal Location 2b'),
        ('Test_Coastal_Workflow3', 'Test Coastal Plot 3', 'Test Coastal Location 3'),
        ('Test_Coastal_Workflow4', 'Test Coastal Plot 4', 'Test Coastal Location 4'),
        ('Test_Coastal_Workflow5', 'Test Coastal Plot 5a', 'Test Coastal Location 5a'),
        ('Test_Coastal_Workflow5', 'Test Coastal Plot 5b', 'Test Coastal Location 5b'),
        ('Test_Coastal_Workflow5', 'Test Coastal Plot 5c', 'Test Coastal Location 5c'),
        ('Span_Workflow', 'SpanPlot', 'Test_Location'),
        ('Partial_Load_Span_Workflow', 'Test Span Plot 9a', 'Test_Location 9a'),
        ('Partial_Load_Span_Workflow', 'Test Span Plot 9b', 'Test_Location 9b'),
        ('Partial_Load_Span_Workflow', 'Test Span Plot 9c', 'Test_Location 9b'),
        ('Span_Workflow_Default_Offset', 'SpanPlot', 'Test_Location'),
        ('Span_Workflow_Multiple_Offsets', 'Multiple Offsets Plot', 'Test_Location'),
        ('Test_Partial_Taskrun_Span_Workflow', 'Test_Partial_Taskrun_Span_Plot', 'Test_Partial_Taskrun_Location'),
        ('Idle_Test_Workflow', 'Idle Test Workflow Plot', 'Test Idle Workflow Location'),
        ('Coastal_Missing_Event_Workflow', 'Coastal_Plot_With_Missing_Events', 'Test Coastal Missing Events Workflow Location'),
        ('Coastal_No_Missing_Events_Workflow', 'Coastal_Plot_Without_Missing_Events', 'Test Coastal No Missing Events Workflow Location')
    `)
  }
  this.beforeEach = async function () {
    await commonTimeseriesTestUtils.beforeEach()
    const request = new sql.Request(pool)
    await request.query('delete from fff_staging.fluvial_display_group_workflow')
  }
  this.afterAll = async function () {
    await commonTimeseriesTestUtils.afterAll()
  }
}
