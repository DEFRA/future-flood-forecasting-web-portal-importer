const sql = require('mssql')
const CommonTimeseriesTestUtils = require('./common-timeseries-test-utils')

module.exports = function (pool, messages) {
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  this.beforeAll = async function () {
    await commonTimeseriesTestUtils.beforeAll(pool)
    const request = new sql.Request(pool)
    await request.batch(`
      insert into
        fff_staging.coastal_display_group_workflow (workflow_id, plot_id, location_ids)
      values
        ('Test_Coastal_Workflow', 'Test Coastal Plot', 'Test Coastal Location'),
        ('Test_Coastal_Workflow1', 'Test Coastal Plot 1', 'Test Coastal Location 1'),
        ('Test_Coastal_Workflow2', 'Test Coastal Plot 2a', 'Test Coastal Location 2a'),
        ('Test_Coastal_Workflow2', 'Test Coastal Plot 2b', 'Test Coastal Location 2b'),
        ('Test_Coastal_Workflow3', 'Test Coastal Plot 3', 'Test Coastal Location 3'),
        ('Test_Coastal_Workflow4', 'Test Coastal Plot 4', 'Test Coastal Location 4'),
        ('Test_Coastal_Workflow5', 'Test Coastal Plot 5', 'Test Coastal Location 5'),
        ('Span_Workflow', 'SpanPlot', 'Test_Location'),
        ('Partial_Load_Span_Workflow', 'Test Span Plot 9a', 'Test_Location 9a'),
        ('Partial_Load_Span_Workflow', 'Test Span Plot 9b', 'Test_Location 9b'),
        ('Partial_Load_Span_Workflow', 'Test Span Plot 9c', 'Test_Location 9b'),
        ('Span_Workflow_Default_Offset', 'SpanPlot', 'Test_Location')
    `)
  }
  this.beforeEach = async function () {
    await commonTimeseriesTestUtils.beforeEach(pool)
  }
  this.afterAll = async function () {
    await commonTimeseriesTestUtils.afterAll(pool)
  }
}
