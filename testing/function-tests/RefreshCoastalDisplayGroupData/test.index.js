const CommonWorkflowCsvTestUtils = require('../shared/common-workflow-csv-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const { doInTransaction } = require('../../../Shared/transaction-helper')
const message = require('../mocks/defaultMessage')
const messageFunction = require('../../../RefreshCoastalDisplayGroupData/index')
const fetch = require('node-fetch')
const sql = require('mssql')
const fs = require('fs')

jest.mock('node-fetch')

module.exports = describe('Insert coastal_display_group_workflow data tests', () => {
  const STATUS_CODE_200 = 200
  const STATUS_TEXT_OK = 'OK'
  const TEXT_CSV = 'text/csv'
  const HTML = 'html'

  let commonWorkflowCsvTestUtils
  let context
  let dummyData

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const request = new sql.Request(pool)

  describe('The refresh coastal_display_group_workflow data function:', () => {
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      const config = {
        csvType: 'C'
      }
      commonWorkflowCsvTestUtils = new CommonWorkflowCsvTestUtils(context, pool, config)
      dummyData = {
        dummyWorkflow: {
          dummyPlot: ['dummyLocation']
        }
      }
      await request.batch('delete from fff_staging.csv_staging_exception')
      await request.batch('delete from fff_staging.staging_exception')
      await request.batch('delete from fff_staging.timeseries_staging_exception')
      await request.batch('delete from fff_staging.timeseries_header')
      await request.query('delete from fff_staging.coastal_display_group_workflow')
      await request.batch('delete from fff_staging.workflow_refresh')
      await request.query('insert into fff_staging.coastal_display_group_workflow (workflow_id, plot_id, location_ids) values (\'dummyWorkflow\', \'dummyPlot\', \'dummyLocation\')')
    })

    afterEach(async () => {
      // As the jestConnectionPool pool is only closed at the end of the test suite the global temporary table used by each function
      // invocation needs to be dropped manually between each test case.
      await request.batch('delete from fff_staging.staging_exception')
      await request.batch('delete from fff_staging.timeseries_staging_exception')
      await request.batch('delete from fff_staging.timeseries_header')
      await request.query('drop table if exists #coastal_display_group_workflow_temp')
    })

    afterAll(async () => {
      await request.query('delete from fff_staging.coastal_display_group_workflow')
      await request.query('delete from fff_staging.csv_staging_exception')
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
        coastalDisplayGroupData: dummyData,
        numberOfExceptionRows: 0
      }

      await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should refresh given a valid CSV file (even with extra csv fields) and replay eligible failed messages', async () => {
      // Ensure messages linked to CSV associated staging exceptions/timeseries staging exceptions are replayed.
      await doInTransaction(insertExceptions, context, 'Error')

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        coastalDisplayGroupData: {
          BE: {
            StringTRITON_outputs_BER: ['St1', 'St2'],
            TRITON_outputs_Other: ['St3']
          },
          Workflow2: {
            Plot3: ['St4']
          }
        },
        numberOfExceptionRows: 0,
        replayedStagingExceptionMessages: ['ukeafffsmc00:000000001 message'],
        replayedTimeseriesStagingExceptionMessages: [
          JSON.parse('{"taskRunId": "ukeafffsmc00:000000003", "plotId": "TRITON_outputs_Other"}'),
          JSON.parse('{"taskRunId": "ukeafffsmc00:000000003", "plotId": "StringTRITON_outputs_BER"}')
        ]
      }

      const expectWorkflowRefresh = true
      await commonWorkflowCsvTestUtils.insertWorkflowRefreshRecords()
      await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData, expectWorkflowRefresh)
    })
    it('should ignore a  CSV file with misspelled headers', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'headers-misspelled.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        coastalDisplayGroupData: dummyData,
        numberOfExceptionRows: 2
      }

      const expectedErrorDescription = 'row is missing data'
      await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should not refresh with valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-header-row-no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        coastalDisplayGroupData: dummyData,
        numberOfExceptionRows: 0
      }

      await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should reject insert if there is no header row, expect the first row to be treated as the header', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-data-rows-no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        coastalDisplayGroupData: dummyData,
        numberOfExceptionRows: 3
      }

      const expectedErrorDescription = 'row is missing data'
      await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should load rows with missing values in columns into exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-data-in-a-column.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        coastalDisplayGroupData: {
          BE: {
            StringTRITON_outputs_BER: ['St2'],
            TRITON_outputs_Other: ['St3']
          },
          Workflow2: {
            Plot3: ['St4']
          }
        },
        numberOfExceptionRows: 1
      }

      const expectWorkflowRefresh = true
      const expectedErrorDescription = 'row is missing data'
      await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData, expectWorkflowRefresh)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should omit all rows as there is missing values for the entire column', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-data-in-entire-column.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        coastalDisplayGroupData: dummyData,
        numberOfExceptionRows: 4
      }

      const expectedErrorDescription = 'row is missing data'
      await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should load a row with fields exceeding data limits into exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'exceeding-data-limit.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        coastalDisplayGroupData: dummyData,
        numberOfExceptionRows: 1
      }

      const expectedErrorDescription = 'data would be truncated.'
      await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should load an incomplete row into exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'incomplete-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        coastalDisplayGroupData: dummyData,
        numberOfExceptionRows: 1
      }

      const expectedErrorDescription = 'row is missing data'
      await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error('connect ECONNREFUSED mockhost')
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
    })
    it('should throw an exception when the coastal_display_group_workflow table is being used', async () => {
      // If the coastal_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      await lockCoastalDisplayGroupTableAndCheckMessageCannotBeProcessed(mockResponseData)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 5000)
    it('should not refresh when a non-csv file (JSON) is provided', async () => {
      const mockResponse = {
        status: STATUS_CODE_200,
        body: fs.createReadStream('testing/function-tests/general-files/json.json'),
        statusText: STATUS_TEXT_OK,
        headers: { 'Content-Type': 'application/javascript' },
        url: '.json'
      }
      await fetch.mockResolvedValue(mockResponse)

      const expectedData = {
        coastalDisplayGroupData: dummyData,
        numberOfExceptionRows: 0
      }
      const expectedError = new Error('No csv file detected')

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData)
    })
    it('should not refresh if csv endpoint is not found(404)', async () => {
      const mockResponse = {
        status: 404,
        body: fs.createReadStream('testing/function-tests/general-files/404.html'),
        statusText: 'Not found',
        headers: { 'Content-Type': HTML },
        url: '.html'
      }
      await fetch.mockResolvedValue(mockResponse)

      const expectedData = {
        coastalDisplayGroupData: dummyData,
        numberOfExceptionRows: 0
      }

      const expectedError = new Error('No csv file detected')

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData)
    })
  })

  async function refreshCoastalDisplayGroupDataAndCheckExpectedResults (mockResponseData, expectedData, expectWorkflowRefresh) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message) // This is a call to the function index
    await checkExpectedResults(expectedData, expectWorkflowRefresh)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/function-tests/RefreshCoastalDisplayGroupData/coastal_display_group_workflow_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false,
      url: '.csv'
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedData, expectWorkflowRefresh) {
    const tableCountResult = await request.query(`
      select 
        count(*) 
      as 
        number 
      from 
        fff_staging.coastal_display_group_workflow`)
    // The number of rows (each workflow - plot combination) returned from the database should be equal to the sum of plot ID elements nested within
    // all workflow ID elements of the expected coastal_display_group_workflow data.
    let expectedNumberOfRows = 0
    for (const workflowId in expectedData.coastalDisplayGroupData) {
      expectedNumberOfRows += Object.keys(expectedData.coastalDisplayGroupData[workflowId]).length
    }

    // Query the database and check that the locations associated with each grouping of workflow ID and plot ID are as expected.
    expect(tableCountResult.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`database row count: ${tableCountResult.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      for (const workflowId in expectedData.coastalDisplayGroupData) { // ident single workflowId within expected data
        const plotIds = expectedData.coastalDisplayGroupData[`${workflowId}`] // ident group of plot ids for workflowId
        for (const plotId in plotIds) {
          const locationIds = plotIds[`${plotId}`] // ident group of location ids for single plotid and single workflowid combination
          const expectedLocationsArray = locationIds.sort()

          // actual db data
          const locationQuery = await request.query(`
            select 
              *
            from 
              fff_staging.coastal_display_group_workflow
            where 
              workflow_id = '${workflowId}' AND plot_id = '${plotId}'
          `)
          const dbRows = locationQuery.recordset
          const dbLocationsResult = dbRows[0].LOCATION_IDS
          const dbLocations = dbLocationsResult.split(';').sort()
          expect(dbLocations).toEqual(expectedLocationsArray)
        }
      }

      if (typeof expectWorkflowRefresh !== 'undefined') {
        // If the CSV table is expected to contain rows other than the row of dummy data check that the workflow refresh table
        // contains a row for the CSV.
        await commonWorkflowCsvTestUtils.checkWorkflowRefreshData(expectWorkflowRefresh)
      }
    }
    // Check exceptions
    const exceptionCount = await request.query(`
      select 
        count(*)
      as 
        number 
      from 
        fff_staging.csv_staging_exception`)

    expect(exceptionCount.recordset[0].number).toBe(expectedData.numberOfExceptionRows || 0)

    // Check messages to be replayed
    await commonWorkflowCsvTestUtils.checkReplayedStagingExceptionMessages(expectedData.replayedStagingExceptionMessages)
    await commonWorkflowCsvTestUtils.checkReplayedTimeseriesStagingExceptionMessages(expectedData.replayedTimeseriesStagingExceptionMessages)
  }

  async function lockCoastalDisplayGroupTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    const tableName = 'coastal_display_group_workflow'
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.query(`
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

  async function checkExceptionIsCorrect (expectedErrorDescription) {
    const result = await request.query(`
      select
        top(1) description
      from
        fff_staging.csv_staging_exception
      order by
        exception_time desc
    `)
    expect(result.recordset[0].description).toContain(expectedErrorDescription)
  }

  async function insertExceptions (transaction, context) {
    await new sql.Request(transaction).batch(`
      declare @id1 uniqueidentifier;
      set @id1 = newid();
      declare @id2 uniqueidentifier;
      set @id2 = newid();
      declare @id3 uniqueidentifier;
      set @id3 = newid();

      insert into
        fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
      values
        ('ukeafffsmc00:000000001 message', 'Missing PI Server input data for Workflow2', 'ukeafffsmc00:000000001', 'P', 'Workflow2', getutcdate());

      insert into
        fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
      values
        ('ukeafffsmc00:000000002 message', 'Missing PI Server input data for Missing Workflow', 'ukeafffsmc00:000000002', 'P', 'Missing Workflow', getutcdate());

      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id1, getutcdate(), getutcdate(), 1, 1, 'ukeafffsmc00:000000003', 'BE', 'message');

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id2, 'TRITON_outputs_Other', 'P', 1, 'C', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "plotId": "TRITON_outputs_Other"}', @id1, 'Error text', dateadd(hour, -1, getutcdate()));

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id3, 'StringTRITON_outputs_BER', 'P', 0, null, 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "plotId": "StringTRITON_outputs_BER"}', @id1, 'Error text', dateadd(hour, -1, getutcdate()));
    `)
  }
})
