const sql = require('mssql')
const CommonTimeseriesTestUtils = require('./common-timeseries-test-utils')

module.exports = function (pool, messages) {
  const commonTimeseriesTestUtils = new CommonTimeseriesTestUtils(pool)
  this.beforeAll = async function () {
    await commonTimeseriesTestUtils.beforeAll(pool)
    const request = new sql.Request(pool)
    await request.batch(`
        insert into
          fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids)
        values
          ('Test_Fluvial_Workflow1', 'Test Fluvial Plot1', 'Test Location1'),
          ('Test_Fluvial_Workflow2', 'Test Fluvial Plot2a', 'Test Location2a'),
          ('Test_Fluvial_Workflow2', 'Test Fluvial Plot2b', 'Test Location2b'),
          ('Span_Workflow', 'SpanPlot', 'Test_Location'),
          ('Span_Workflow2', 'SpanPlot2', 'Test_Location2')
      `)
  }
  this.beforeEach = async function () {
    await commonTimeseriesTestUtils.beforeEach(pool)
  }
  this.afterAll = async function () {
    await commonTimeseriesTestUtils.afterAll(pool)
  }
}
