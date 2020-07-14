const sql = require('mssql')

module.exports = function (pool) {
  const deleteWorkflowData = async function (request) {
    await request.batch(`delete from fff_staging.coastal_display_group_workflow`)
    await request.batch(`delete from fff_staging.fluvial_display_group_workflow`)
    await request.batch(`delete from fff_staging.non_display_group_workflow`)
    await request.batch(`delete from fff_staging.ignored_workflow`)
  }
  const deleteTimeseriesData = async function (request) {
    await request.batch(`delete from fff_staging.timeseries`)
    await request.batch(`delete from fff_staging.timeseries_header`)
    await request.batch(`delete from fff_staging.staging_exception`)
  }
  this.beforeAll = async function () {
    const request = new sql.Request(pool)
    await pool.connect()
    await deleteWorkflowData(request)
  }
  this.beforeEach = async function () {
    // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
    // function implementation for the function context needs creating for each test.
    const request = new sql.Request(pool)
    await deleteTimeseriesData(request)
  }
  this.afterAll = async function () {
    const request = new sql.Request(pool)
    await deleteWorkflowData(request)
    await deleteTimeseriesData(request)
    // Closing the DB connection allows Jest to exit successfully.
    await pool.close()
  }
}
