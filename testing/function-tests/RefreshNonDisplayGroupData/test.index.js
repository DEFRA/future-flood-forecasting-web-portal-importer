const CommonCsvRefreshUtils = require('../shared/common-csv-refresh-utils')
const CommonWorkflowCsvTestUtils = require('../shared/common-workflow-csv-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const { doInTransaction } = require('../../../Shared/transaction-helper')
const message = require('../mocks/defaultMessage')
const messageFunction = require('../../../RefreshNonDisplayGroupData/index')
const fetch = require('node-fetch')
const sql = require('mssql')
const fs = require('fs')

jest.mock('node-fetch')

module.exports = describe('Insert non_display_group_workflow data tests', () => {
  const JSONFILE = 'application/javascript'
  const STATUS_CODE_200 = 200
  const STATUS_TEXT_OK = 'OK'
  const TEXT_CSV = 'text/csv'
  const HTML = 'html'

  let commonCsvRefreshUtils
  let commonWorkflowCsvTestUtils
  let context
  let dummyData

  const EXTERNAL_HISTORICAL = 'external_historical'

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const request = new sql.Request(pool)

  describe('The refresh non_display_group_workflow data function', () => {
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      const config = {
        csvType: 'N'
      }
      commonCsvRefreshUtils = new CommonCsvRefreshUtils(context)
      commonWorkflowCsvTestUtils = new CommonWorkflowCsvTestUtils(context, pool, config)
      dummyData = {
        dummyWorkflow: [{ filterId: 'dummyFilter', approved: 0, startTimeOffset: 1, endTimeOffset: 2, timeSeriesType: EXTERNAL_HISTORICAL }]
      }
      await request.batch(`delete from fff_staging.csv_staging_exception`)
      await request.batch(`delete from fff_staging.staging_exception`)
      await request.batch(`delete from fff_staging.timeseries_staging_exception`)
      await request.batch(`delete from fff_staging.non_display_group_workflow`)
      await request.batch(`delete from fff_staging.workflow_refresh`)
      await request.batch(`
          insert into
            fff_staging.non_display_group_workflow
              (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
          values
            ('dummyWorkflow', 'dummyFilter', 0, 1, 2, 'external_historical')`)
    })

    afterAll(async () => {
      await request.batch(`delete from fff_staging.non_display_group_workflow`)
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
        nonDisplayGroupData: dummyData,
        numberOfExceptionRows: 0
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should load a valid csv correctly - single filter per workflow', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'single-filter-per-workflow.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedNonDisplayGroupData = {
        test_non_display_workflow_1: [{ filterId: 'test_filter_1', approved: 0, startTimeOffset: 10, endTimeOffset: 20, timeSeriesType: EXTERNAL_HISTORICAL }],
        test_non_display_workflow_3: [{ filterId: 'test_filter_3', approved: 0, startTimeOffset: 5, endTimeOffset: 10, timeSeriesType: EXTERNAL_HISTORICAL }],
        test_non_display_workflow_2: [{ filterId: 'test_filter_2', approved: 1, startTimeOffset: 1, endTimeOffset: 2, timeSeriesType: EXTERNAL_HISTORICAL }]
      }

      const expectedData = {
        nonDisplayGroupData: expectedNonDisplayGroupData,
        numberOfExceptionRows: 0
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should load a valid csv correctly - multiple filters per workflow and replay eligible failed messages', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'multiple-filters-per-workflow.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedNonDisplayGroupData = {
        test_non_display_workflow_1: [{ filterId: 'test_filter_1', approved: 0, startTimeOffset: 5, endTimeOffset: 10, timeSeriesType: EXTERNAL_HISTORICAL }, { filterId: 'test_filter_1a', approved: 1, startTimeOffset: 6, endTimeOffset: 9, timeSeriesType: EXTERNAL_HISTORICAL }],
        test_non_display_workflow_3: [{ filterId: 'test_filter_3', approved: 1, startTimeOffset: 5, endTimeOffset: 10, timeSeriesType: EXTERNAL_HISTORICAL }],
        test_non_display_workflow_2: [{ filterId: 'test_filter_2', approved: 0, startTimeOffset: 5, endTimeOffset: 10, timeSeriesType: EXTERNAL_HISTORICAL }]
      }

      const expectedData = {
        nonDisplayGroupData: expectedNonDisplayGroupData,
        numberOfExceptionRows: 0,
        replayedStagingExceptionMessages: ['ukeafffsmc00:000000001 message'],
        replayedTimeseriesStagingExceptionMessages: [
          JSON.parse('{"taskRunId": "ukeafffsmc00:000000003", "filterId": "test_filter_1"}'),
          JSON.parse('{"taskRunId": "ukeafffsmc00:000000003", "filterId": "test_filter_1a"}')
        ]
      }

      // Ensure messages linked to CSV associated staging exceptions/timeseries staging exceptions are replayed.
      await doInTransaction(insertExceptions, context, 'Error')

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should not load duplicate rows in a csv', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'duplicate-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedNonDisplayGroupData = {
        test_non_display_workflow_1: [{ filterId: 'test_filter_1', approved: 0, startTimeOffset: 5, endTimeOffset: 10, timeSeriesType: EXTERNAL_HISTORICAL }],
        test_non_display_workflow_3: [{ filterId: 'test_filter_3', approved: 0, startTimeOffset: 5, endTimeOffset: 10, timeSeriesType: EXTERNAL_HISTORICAL }],
        test_non_display_workflow_2: [{ filterId: 'test_filter_2', approved: 1, startTimeOffset: 5, endTimeOffset: 10, timeSeriesType: EXTERNAL_HISTORICAL }]
      }
      const expectedErrorDescription = 'Violation of UNIQUE KEY constraint'

      const expectedData = {
        nonDisplayGroupData: expectedNonDisplayGroupData,
        numberOfExceptionRows: 1
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should ignore a CSV file with misspelled headers', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'headers-misspelled.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedErrorDescription = 'row is missing data.'
      const expectedData = {
        nonDisplayGroupData: dummyData,
        numberOfExceptionRows: 3
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should load WorkflowId and filterId correctly into the db correctly, even with extra CSV fields present', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'extra-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedNonDisplayGroupData = {
        test_non_display_workflow_1: [{ filterId: 'test_filter_1', approved: 0, startTimeOffset: 3, endTimeOffset: 2, timeSeriesType: EXTERNAL_HISTORICAL }],
        test_non_display_workflow_2: [{ filterId: 'test_filter_2', approved: 0, startTimeOffset: 3, endTimeOffset: 2, timeSeriesType: EXTERNAL_HISTORICAL }]
      }

      const expectedData = {
        nonDisplayGroupData: expectedNonDisplayGroupData,
        numberOfExceptionRows: 0
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should not refresh with valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-header-row-no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        nonDisplayGroupData: dummyData,
        numberOfExceptionRows: 0
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should reject insert if there is no header row, expect the first row to be treated as the header', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-data-rows-no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        nonDisplayGroupData: dummyData,
        numberOfExceptionRows: 2
      }

      const expectedErrorDescription = 'row is missing data.'

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should omit rows with missing values', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-data-in-some-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedErrorDescription = 'row is missing data.'

      const expectedNonDisplayGroupData = {
        test_non_display_workflow_2: [{ filterId: 'test_filter_a', approved: 0, startTimeOffset: 0, endTimeOffset: 0, timeSeriesType: EXTERNAL_HISTORICAL }]
      }

      const expectedData = {
        nonDisplayGroupData: expectedNonDisplayGroupData,
        numberOfExceptionRows: 1
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should omit all rows as there is missing values for the entire column', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-data-in-entire-column.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedErrorDescription = 'row is missing data.'
      const expectedData = {
        nonDisplayGroupData: dummyData,
        numberOfExceptionRows: 3
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should not refresh when a non-csv file (JSON) is provided', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'json-file.json',
        statusText: STATUS_TEXT_OK,
        contentType: JSONFILE
      }

      const expectedData = {
        nonDisplayGroupData: dummyData
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should not refresh if csv endpoint is not found(404)', async () => {
      const mockResponseData = {
        status: 404,
        body: fs.createReadStream(`testing/function-tests/general-files/404.html`),
        statusText: 'Not found',
        headers: { 'Content-Type': HTML },
        url: '.html'
      }
      await fetch.mockResolvedValue(mockResponseData)

      const expectedError = new Error(`No csv file detected`)
      const expectedData = {
        nonDisplayGroupData: dummyData,
        numberOfExceptionRows: 0
      }

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData)
    })
    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error(`connect ECONNREFUSED mockhost`)
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
    })
    it('should throw an exception when the non_display_group_workflow table is being used', async () => {
      // If the non_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'single-filter-per-workflow.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }
      await lockNonDisplayGroupTableAndCheckMessageCannotBeProcessed(mockResponseData)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    it('should load unloadable rows into csv exceptions table', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'invalid-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }
      const expectedNonDisplayGroupData = {
        test_non_display_workflow_1: [{ filterId: 'test_filter_1', approved: 0, startTimeOffset: 0, endTimeOffset: 0, timeSeriesType: EXTERNAL_HISTORICAL }]
      }
      const expectedErrorDescription = 'row is missing data.'
      const expectedData = {
        nonDisplayGroupData: expectedNonDisplayGroupData,
        numberOfExceptionRows: 1
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should only load valid rows within a csv correctly. bit instead of boolean row loaded into exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'bit-not-boolean.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedNonDisplayGroupData = {
        test_non_display_workflow_3: [{ filterId: 'test_filter_3', approved: 0, startTimeOffset: 0, endTimeOffset: 0, timeSeriesType: EXTERNAL_HISTORICAL }],
        test_non_display_workflow_2: [{ filterId: 'test_filter_2', approved: 1, startTimeOffset: 0, endTimeOffset: 0, timeSeriesType: EXTERNAL_HISTORICAL }]
      }

      const expectedData = {
        nonDisplayGroupData: expectedNonDisplayGroupData,
        numberOfExceptionRows: 1
      }

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
  })

  async function refreshNonDisplayGroupDataAndCheckExpectedResults (mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message)
    await checkExpectedResults(expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/function-tests/RefreshNonDisplayGroupData/non_display_group_workflow_files/${mockResponseData.filename}`),
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
        fff_staging.non_display_group_workflow`)
    const workflowIds = Object.keys(expectedData.nonDisplayGroupData)
    let expectedNumberOfRows = 0

    // The number of rows returned from the database should be equal to the sum of the elements nested within the expected non_display_group_workflow expected data.
    for (const workflowId of workflowIds) {
      expectedNumberOfRows += Object.keys(expectedData.nonDisplayGroupData[workflowId]).length
    }

    // Query the database and check that the filter IDs associated with each workflow ID are as expected.
    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`databse row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      const workflowIds = Object.keys(expectedData.nonDisplayGroupData)
      for (const workflowId of workflowIds) { // ident single workflowId within expected data
        const expectedWorkflowData = expectedData.nonDisplayGroupData[`${workflowId}`]

        // actual db data
        const filterQuery = await request.query(`
          select 
            filter_id,
            cast(approved as int) as approved,
            start_time_offset_hours as startTimeOffset,
            end_time_offset_hours as endTimeOffset,
            timeseries_type as timeSeriesType
          from 
            fff_staging.non_display_group_workflow
          where 
            workflow_id = '${workflowId}'
          order by
            filter_id
          `)
        const rows = filterQuery.recordset
        const dbData = []
        rows.forEach(row =>
          dbData.push({ filterId: row.filter_id, approved: row.approved, startTimeOffset: row.startTimeOffset, endTimeOffset: row.endTimeOffset, timeSeriesType: row.timeSeriesType })
        )
        const expectedWorkflowDataSorted = expectedWorkflowData.sort()
        // get an array of filter ids for a given workflow id from the database
        expect(dbData).toEqual(expectedWorkflowDataSorted)
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
  async function lockNonDisplayGroupTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    const tableName = 'non_display_group_workflow'
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(`
        insert into
          fff_staging.${tableName}
            (workflow_id, filter_id, approved, start_time_offset_hours, end_time_offset_hours, timeseries_type)
        values
          ('testWorkflow', 'testFilter',0,0,0,'external_historical')`)
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
      declare @id4 uniqueidentifier;
      set @id4 = newid();

      insert into
        fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
      values
        ('ukeafffsmc00:000000001 message', 'Missing PI Server input data for test_non_display_workflow_2', 'ukeafffsmc00:000000001', 'P', 'test_non_display_workflow_2', getutcdate());

      insert into
        fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
      values
        ('ukeafffsmc00:000000002 message', 'Missing PI Server input data for Missing Workflow', 'ukeafffsmc00:000000002', 'P', 'Missing Workflow', getutcdate());

      insert into fff_staging.timeseries_header
        (id, task_start_time, task_completion_time, forecast, approved, task_run_id, workflow_id, message)
      values
        (@id1, getutcdate(), getutcdate(), 1, 1, 'ukeafffsmc00:000000003', 'test_non_display_workflow_1', 'message');

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id2, 'test_filter_1', 'F', 1, 'N', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "filterId": "test_filter_1"}', @id1, 'Error text', dateadd(hour, -1, getutcdate()));

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id3, 'test_filter_1a', 'F', 1, 'N', 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000003", "filterId": "test_filter_1a"}', @id1, 'Error text', dateadd(hour, -1, getutcdate()));

      insert into fff_staging.timeseries_staging_exception
        (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description, exception_time)
      values
        (@id4, 'test_filter_1', 'F', 0, null, 'fews_parameters', '{"taskRunId": "ukeafffsmc00:000000004", "filterId": "test_filter_3"}', @id1, 'Error text', dateadd(hour, -1, getutcdate()));
    `)
  }
})
