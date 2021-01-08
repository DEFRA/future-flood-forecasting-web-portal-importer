const sql = require('mssql')

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
  coastalDisplayGroupWorkflow: {
    query: COASTAL_DISPLAY_GROUP_WORKFLOW_LOCK_TIMEOUT_QUERY,
    tableName: 'coastal_display_group_workflow'
  },
  fluvialDisplayGroupWorkflow: {
    query: FLUVIAL_DISPLAY_GROUP_WORKFLOW_LOCK_TIMEOUT_QUERY,
    tableName: 'fluvial_display_group_workflow'
  },
  nonDisplayGroupWorkflow: {
    query: NON_DISPLAY_GROUP_WORKFLOW_LOCK_TIMEOUT_QUERY,
    tableName: 'non_display_group_workflow'
  },
  ignoredWorkflow: {
    query: IGNORED_WORKFLOW_LOCK_TIMEOUT_QUERY,
    tableName: 'ignored_display_group_workflow'
  }
}

module.exports = function (pool) {
  const deleteWorkflowData = async function (request) {
    await request.batch('delete from fff_staging.coastal_display_group_workflow')
    await request.batch('delete from fff_staging.fluvial_display_group_workflow')
    await request.batch('delete from fff_staging.non_display_group_workflow')
    await request.batch('delete from fff_staging.ignored_workflow')
    await request.batch('delete from fff_staging.workflow_refresh')
  }
  const deleteTimeseriesData = async function (request) {
    await request.batch('delete from fff_staging.inactive_timeseries_staging_exception')
    await request.batch('delete from fff_staging.timeseries_staging_exception')
    await request.batch('delete from fff_staging.timeseries')
    await request.batch('delete from fff_staging.timeseries_header')
    await request.batch('delete from fff_staging.inactive_staging_exception')
    await request.batch('delete from fff_staging.staging_exception')
  }
  this.beforeAll = async function () {
    const request = new sql.Request(pool)
    await pool.connect()
    await deleteWorkflowData(request)
    await request.batch(`
      insert into
        fff_staging.ignored_workflow (workflow_id)
      values
        ('Test_Ignored_Workflow_1'),
        ('Test_Ignored_Workflow_2')
    `)

    await request.batch(`
      insert into
        fff_staging.workflow_refresh (csv_type)
      values
        ('C'),
        ('F'),
        ('N'),
        ('I')
    `)
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
  this.checkNumberOfActiveStagingExceptionsForSourceFunctionOfWorkflow = async function (config) {
    const request = new sql.Request(pool)
    await request.input('taskRunId', sql.NVarChar, config.taskRunId)
    await request.input('sourceFunction', sql.NVarChar, config.sourceFunction)
    const result = await request.query(`
      select
        count(id) as number
      from
        fff_staging.v_active_staging_exception
      where
        source_function = @sourceFunction and
        workflow_id = (
          select
            workflow_id
          from
            fff_staging.timeseries_header
          where
            task_run_id = @taskRunId
        )
    `)
    expect(result.recordset[0].number).toBe(config.expectedNumberOfStagingExceptions || 0)
  }
  this.checkNumberOfActiveTimeseriesStagingExceptionsForWorkflowOfTaskRun = async function (config) {
    const request = new sql.Request(pool)
    await request.input('taskRunId', sql.NVarChar, config.taskRunId)
    const result = await request.query(`
      select
        count(tse.id) as number
      from
        fff_staging.v_active_timeseries_staging_exception tse
      where
        tse.timeseries_header_id in (
          select
            th.id
          from
            fff_staging.timeseries_header th
          where
            workflow_id = (
              select
              th2.workflow_id
              from
                fff_staging.timeseries_header th2
              where
                th2.task_run_id = @taskRunId
            )
        )
    `)
    expect(result.recordset[0].number).toBe(config.expectedNumberOfTimeseriesStagingExceptions || 0)
  }
  this.lockWorkflowTableAndCheckMessageCannotBeProcessed = async function (config) {
    let transaction
    try {
      // Lock the timeseries table and then try and process the message.
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(lockTimeoutData[config.workflow].query)
      await expect(config.processMessageFunction(config.context, config.message)).rejects.toBeTimeoutError(lockTimeoutData[config.workflow].tableName)
    } finally {
      if (transaction._aborted) {
        config.context.log.warn('The transaction has been aborted.')
      } else {
        await transaction.rollback()
        config.context.log.warn('The transaction has been rolled back.')
      }
    }
  }
  this.setOptionalDatabaseEnvironmentVariables = async function () {
    process.env.SQLDB_ABORT_TRANSACTION_ON_ERROR = false
    process.env.SQLDB_PORT = 1433
    process.env.SQLDB_CONNECTION_TIMEOUT_MILLIS = 15000
    process.env.SQLDB_REQUEST_TIMEOUT_MILLIS = 60000
    process.env.SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS = 3
    process.env.SQLDB_PACKET_SIZE = 4096
    process.env.SQLDB_MAX_POOLED_CONNECTIONS = 10
    process.env.SQLDB_MIN_POOLED_CONNECTIONS = 6
    process.env.SQLDB_ACQUIRE_TIMEOUT_MILLIS = 30000
    process.env.SQLDB_CREATE_TIMEOUT_MILLIS = 30000
    process.env.SQLDB_DESTROY_TIMEOUT_MILLIS = 5000
    process.env.SQLDB_IDLE_TIMEOUT_MILLIS = 30000
    process.env.SQLDB_REAP_INTERVAL_MILLIS = 1000
    process.env.SQLDB_CREATE_RETRY_INTERVAL_MILLIS = 200
  }
}
