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
          ('Test_Fluvial_Workflow3', 'Test Fluvial Plot3', 'Test Location3a;Test Location3b;Test Location3c;Test Location3d'),
          ('Test_Fluvial_Workflow4', 'Test Fluvial Plot4', 'Test Location3c;Test Location3d'),
          ('Test_Fluvial_Workflow5', 'Test Fluvial Plot5', 'Test Location3a;Test Location3b;Test Location3c;Test Location3d'),
          ('Span_Workflow2', 'SpanPlot2', 'Test_Location'),
          ('Span_Workflow3', 'SpanPlot3', 'Test_Location;Test_Location2')
      `)
  }
  this.beforeEach = async function () {
    await commonTimeseriesTestUtils.beforeEach(pool)
  }
  this.afterAll = async function () {
    await commonTimeseriesTestUtils.afterAll(pool)
  }
}
