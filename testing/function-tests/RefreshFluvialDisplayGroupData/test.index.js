const CommonCsvRefreshUtils = require('../shared/common-csv-refresh-utils')
const CommonWorkflowCsvTestUtils = require('../shared/common-workflow-csv-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const { doInTransaction } = require('../../../Shared/transaction-helper')
const message = require('../mocks/defaultMessage')
const messageFunction = require('../../../RefreshFluvialDisplayGroupData/index')
const fetch = require('node-fetch')
const sql = require('mssql')
const fs = require('fs')

jest.mock('node-fetch')

module.exports = describe('Insert fluvial_display_group_workflow data tests', () => {
  const STATUS_CODE_200 = 200
  const STATUS_TEXT_OK = 'OK'
  const TEXT_CSV = 'text/csv'
  const HTML = 'html'

  let commonCsvRefreshUtils
  let commonWorkflowCsvTestUtils
  let context
  let dummyData

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const request = new sql.Request(pool)

  describe('The refresh fluvial_display_group_workflow data function:', () => {
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      const config = {
        csvType: 'F'
      }
      commonCsvRefreshUtils = new CommonCsvRefreshUtils(context)
      commonWorkflowCsvTestUtils = new CommonWorkflowCsvTestUtils(context, pool, config)
      dummyData = {
        dummyWorkflow: {
          dummyPlot: ['dummyLocation']
        }
      }
      await request.batch(`delete from fff_staging.csv_staging_exception`)
      await request.batch(`delete from fff_staging.staging_exception`)
      await request.batch(`delete from fff_staging.timeseries_staging_exception`)
      await request.batch(`delete from fff_staging.fluvial_display_group_workflow`)
      await request.batch(`delete from fff_staging.workflow_refresh`)
      await request.batch(`insert into fff_staging.fluvial_display_group_workflow (workflow_id, plot_id, location_ids) values ('dummyWorkflow', 'dummyPlot', 'dummyLocation')`)
    })

    afterEach(async () => {
      await request.batch(`drop table if exists #fluvial_display_group_workflow_temp`)
    })

    afterAll(async () => {
      await request.batch(`delete from fff_staging.fluvial_display_group_workflow`)
      await request.batch(`delete from fff_staging.csv_staging_exception`)
      // Closing the DB connection allows Jest to exit successfully.
      await pool.close()
    })

    it('should ignore an empty CSV file', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'empty.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        displayGroupData: dummyData
      }

      await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })

    it('should ignore a CSV file with misspelled headers', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'headers-misspelled.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        displayGroupData: dummyData,
        numberOfExceptionRows: 3
      }

      await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })

    it('should load only PlotId, FFFSLocID and WorkflowId into the db correctly, ignoring extra CSV fields', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'extra-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        displayGroupData: {
          workflow1: {
            plot1: ['location1', 'location2']
          }
        }
      }
      await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })

    it('should group locations by plot ID and workflow ID given single location per workflowId/plotId and replay eligible failed messages', async () => {
      await commonWorkflowCsvTestUtils.insertWorkflowRefreshRecords(-600)
      // Ensure messages linked to CSV associated staging exceptions/timeseries staging exceptions are replayed.
      await doInTransaction(insertExceptions, context, 'Error')

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'single-location-per-plot-for-workflow.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        displayGroupData: {
          workflow1: {
            plot1: ['location4'],
            plot2: ['location1']
          },
          workflow2: {
            plot1: ['location1']
          }
        },
        replayedStagingExceptionMessages: ['ukeafffsmc00:000000001 message', 'ukeafffsmc00:000000004 message'],
        replayedTimeseriesStagingExceptionMessages: [
          JSON.parse('{"taskRunId": "ukeafffsmc00:000000003", "plotId": "plot1"}'),
          JSON.parse('{"taskRunId": "ukeafffsmc00:000000003", "plotId": "plot2"}')
        ]
      }

      await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })

    it('should group locations by plot ID and workflow ID given multiple combinations of workflowId and plotId', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'multiple-locations-per-plot-for-workflow.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        displayGroupData: {
          workflow1: {
            plot1: ['location1', 'location2', 'location3', 'location4'],
            plot2: ['location1']
          },
          workflow2: {
            plot1: ['location1', 'location2']
          }
        }
      }

      await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })

    it('should not refresh with valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-header-row-no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        displayGroupData: dummyData
      }

      await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })

    it('should reject insert if there is no header row, expect the first row to be treated as the header', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-data-rows-no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        displayGroupData: dummyData,
        numberOfExceptionRows: 2
      }

      await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })

    it('should ommit rows with missing values in columns', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-data-in-columns.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        displayGroupData: {
          workflow2: {
            plot1: ['location1']
          }
        },
        numberOfExceptionRows: 2
      }

      await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })

    it('should omit all rows as there is missing values for the entire column', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-data-in-entire-column.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        displayGroupData: dummyData,
        numberOfExceptionRows: 3
      }

      await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })

    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error(`connect ECONNREFUSED mockhost`)
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
    })

    it('should throw an exception when the fluvial_display_group_workflow table is being used', async () => {
      // If the fluvial_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'multiple-locations-per-plot-for-workflow.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      await lockWorkflowTableAndCheckMessageCannotBeProcessed(mockResponseData)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)

    it('should load unloadable rows into csv exceptions table', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'invalid-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedErrorDescription = 'String or binary data would be truncated.'

      await refreshDisplayGroupDataAndCheckExceptionIsCreated(mockResponseData, expectedErrorDescription)
    })

    it('should not refresh when a non-csv file (JSON) is provided', async () => {
      const mockResponse = {
        status: STATUS_CODE_200,
        body: fs.createReadStream(`testing/function-tests/general-files/json.json`),
        statusText: STATUS_TEXT_OK,
        headers: { 'Content-Type': 'application/javascript' },
        url: '.json'
      }
      await fetch.mockResolvedValue(mockResponse)

      const expectedData = {
        displayGroupData: dummyData,
        numberOfExceptionRows: 0
      }

      const expectedError = new Error(`No csv file detected`)

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData)
    })
    it('should not refresh if csv endpoint is not found(404)', async () => {
      const mockResponse = {
        status: 404,
        body: fs.createReadStream(`testing/function-tests/general-files/404.html`),
        statusText: 'Not found',
        headers: { 'Content-Type': HTML },
        url: '.html'
      }
      await fetch.mockResolvedValue(mockResponse)

      const expectedData = {
        displayGroupData: dummyData,
        numberOfExceptionRows: 0
      }

      const expectedError = new Error(`No csv file detected`)

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData)
    })
  })

  async function refreshDisplayGroupDataAndCheckExpectedResults (mockResponseData, expectedData) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message)
    await checkExpectedResults(expectedData)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/function-tests/RefreshFluvialDisplayGroupData/fluvial_display_group_workflow_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false,
      url: '.csv'
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedData) {
    const result = await request.query(`
      select 
        count(*) 
      as 
        number 
      from 
        fff_staging.fluvial_display_group_workflow`)
    const workflowIds = Object.keys(expectedData.displayGroupData)
    let expectedNumberOfRows = 0

    // The number of rows returned from the database should be equal to the sum of plot ID elements nested within
    // all workflow ID elements of the expected fluvial_display_group_workflow data.
    for (const workflowId of workflowIds) {
      expectedNumberOfRows += Object.keys(expectedData.displayGroupData[workflowId]).length
    }

    // Query the database and check that the locations associated with each grouping of workflow ID and plot ID are as expected.
    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`databse row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      const workflowIds = Object.keys(expectedData.displayGroupData)
      for (const workflowId of workflowIds) { // ident single workflowId within expected data
        const plotIds = expectedData.displayGroupData[`${workflowId}`] // ident group of plot ids for workflowId
        for (const plotId in plotIds) { // ident single plot id within workflowId to access locations
          // expected data layout
          const locationIds = plotIds[`${plotId}`] // ident group of location ids for single plotid and single workflowid combination
          const expectedLocationsArray = locationIds.sort()

          // actual db data
          const locationQuery = await request.query(`
            select
              *
            from 
              fff_staging.fluvial_display_group_workflow
            where 
              workflow_id = '${workflowId}' AND plot_id = '${plotId}'
          `)
          const rows = locationQuery.recordset
          const dbLocationsResult = rows[0].LOCATION_IDS
          const dbLocations = dbLocationsResult.split(';').sort()
          expect(dbLocations).toEqual(expectedLocationsArray)
        }
      }
      if (expectedNumberOfRows > 1) {
        // If the CSV table is expected to contain rows other than the row of dummy data check that the workflow refresh table
        // contains a row for the CSV.
        await commonWorkflowCsvTestUtils.checkWorkflowRefreshData()
      }
    }
    // Check exceptions
    const exceptionCount = await request.query(`
      select
        count(*)
      as
        number
      from
        fff_staging.csv_staging_exception
    `)

    expect(exceptionCount.recordset[0].number).toBe(expectedData.numberOfExceptionRows || 0)

    // Check messages to be replayed
    await commonCsvRefreshUtils.checkReplayedStagingExceptionMessages(expectedData.replayedStagingExceptionMessages)
    await commonCsvRefreshUtils.checkReplayedTimeseriesStagingExceptionMessages(expectedData.replayedTimeseriesStagingExceptionMessages)
  }

  async function lockWorkflowTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    const tableName = 'fluvial_display_group_workflow'
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(`
        insert into 
          fff_staging.${tableName} (workflow_id, plot_id, location_ids)
        values 
         ('workflow_id', 'plot_id', 'loc_id')
      `)
      await mockFetchResponse(mockResponseData)
      await expect(messageFunction(context, message)).rejects.toBeTimeoutError(tableName)
    } finally {
      if (transaction._aborted) {
        context.log.warn('The transaction has been aborted.')
      } else {
        await transaction.rollback()
        context.log.warn('The transaction has been rolled back.')
      }
    }
  }

  async function refreshDisplayGroupDataAndCheckExceptionIsCreated (mockResponseData, expectedErrorDescription) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message) // This is a call to the function index
    const result = await request.query(`
      select
        top(1) description
      from
        fff_staging.csv_staging_exception
      order by
        exception_time desc
    `)
    expect(result.recordset[0].description).toBe(expectedErrorDescription)
  }

  async function insertExceptions (transaction, context) {
    await new sql.Request(transaction).batch(`
      declare @id1 uniqueidentifier;
      set @id1 = newid();
      declare @id2 uniqueidentifier;
      set @id2 = newid();
      declare @id3 uniqueidentifier;
      set @id3 = newid();
      declare @id4 uniqueidentifier;
      set @id4 = newid();
      declare @id5 uniqueidentifier;
      set @id5 = newid();
      declare @id6 uniqueidentifier;
      set @id6 = newid();
      declare @id7 uniqueidentifier;
      set @id7 = newid();
      declare @id8 uniqueidentifier;
      set @id8 = newid();

      insert into
        fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
      values
        ('ukeafffsmc00:000000001 message', 'Missing PI Server input data for workflow1', 'ukeafffsmc00:000000001', 'P', 'workflow1', getutcdate());

      insert into
        fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
      values
        ('ukeafffsmc00:000000002 message', 'Missing PI Server input data for Missing Workflow', 'ukeafffsmc00:000000002', 'P', 'Missing Workflow', getutcdate());

      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id1, getutcdate(), getutcdate(), 1, 1, 'ukeafffsmc00:000000003', 'workflow1', 'ukeafffsmc00:000000003 message');

      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id2, getutcdate(), getutcdate(), 1, 1, 'ukeafffsmc00:000000004', 'workflow1', 'ukeafffsmc00:000000004 message');

      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id3, getutcdate(), getutcdate(), 1, 1, 'ukeafffsmc00:000000005', 'workflow2', 'ukeafffsmc00:000000005 message');

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id4, 'plot1', 'P', 1, 'F', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "plotId": "plot1"}', @id1, 'Error text', dateadd(hour, -1, getutcdate()));

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id5, 'plot2', 'P', 1, 'F', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "plotId": "plot2"}', @id1, 'Error text', dateadd(hour, -1, getutcdate()));

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id6, 'plot1 with typo', 'P', 1, 'F', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000004", "plotId": "plot1 with typo"}', @id2, 'Error text', dateadd(hour, -1, getutcdate()));

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id7, 'plot2 with typo', 'P', 1, 'F', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000004", "plotId": "plot2 with typo"}', @id2, 'Error text', dateadd(hour, -1, getutcdate()));

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id8, 'plot1', 'P', 0, null, 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000005", "plotId": "plot1"}', @id3, 'Error text', dateadd(hour, -1, getutcdate()));
    `)
  }
})
