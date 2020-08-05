const messageFunction = require('../../../RefreshNonDisplayGroupData/index')
const ConnectionPool = require('../../../Shared/connection-pool')
const message = require('../mocks/defaultMessage')
const Context = require('../mocks/defaultContext')
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
      dummyData = {
        dummyWorkflow: [{ filterId: 'dummyFilter', approved: 0, startTimeOffset: 1, endTimeOffset: 2, timeSeriesType: EXTERNAL_HISTORICAL }]
      }
      await request.batch(`delete from fff_staging.csv_staging_exception`)
      await request.batch(`delete from fff_staging.non_display_group_workflow`)
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

      const expectedNonDisplayGroupData = dummyData
      const expectedNumberOfExceptionRows = 0
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
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

      const expectedNumberOfExceptionRows = 0
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
    })
    it('should load a valid csv correctly - multiple filters per workflow', async () => {
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

      const expectedNumberOfExceptionRows = 0
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
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
      const expectedNumberOfExceptionRows = 1
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should ignore a CSV file with misspelled headers', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'headers-misspelled.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedNonDisplayGroupData = dummyData
      const expectedErrorDescription = 'row is missing data.'
      const expectedNumberOfExceptionRows = 3
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
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

      const expectedNumberOfExceptionRows = 0
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
    })
    it('should not refresh with valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-header-row-no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedNonDisplayGroupData = dummyData

      const expectedNumberOfExceptionRows = 0
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
    })
    it('should reject insert if there is no header row, expect the first row to be treated as the header', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-data-rows-no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedNonDisplayGroupData = dummyData

      const expectedErrorDescription = 'row is missing data.'
      const expectedNumberOfExceptionRows = 2
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should omit rows with missing values', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-data-in-some-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedNonDisplayGroupData = {
        test_non_display_workflow_2: [{ filterId: 'test_filter_a', approved: 0, startTimeOffset: 0, endTimeOffset: 0, timeSeriesType: EXTERNAL_HISTORICAL }]
      }

      const expectedErrorDescription = 'row is missing data.'
      const expectedNumberOfExceptionRows = 1
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
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
      const expectedNonDisplayGroupData = dummyData
      const expectedNumberOfExceptionRows = 3
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
      await checkExceptionIsCorrect(expectedErrorDescription)
    })
    it('should not refresh when a non-csv file (JSON) is provided', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'json-file.json',
        statusText: STATUS_TEXT_OK,
        contentType: JSONFILE
      }

      const expectedNonDisplayGroupData = dummyData

      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
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

      const expectedData = dummyData
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error(`No csv file detected`)

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData, expectedNumberOfExceptionRows)
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
      const expectedNumberOfExceptionRows = 1
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
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

      const expectedNumberOfExceptionRows = 1
      await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData, expectedNumberOfExceptionRows)
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

  async function checkExpectedResults (expectedNonDisplayGroupData, expectedNumberOfExceptionRows) {
    const result = await request.query(`
      select 
        count(*) 
      as 
        number 
      from 
        fff_staging.non_display_group_workflow`)
    const workflowIds = Object.keys(expectedNonDisplayGroupData)
    let expectedNumberOfRows = 0

    // The number of rows returned from the database should be equal to the sum of the elements nested within the expected non_display_group_workflow expected data.
    for (const workflowId of workflowIds) {
      expectedNumberOfRows += Object.keys(expectedNonDisplayGroupData[workflowId]).length
    }

    // Query the database and check that the filter IDs associated with each workflow ID are as expected.
    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`databse row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      const workflowIds = Object.keys(expectedNonDisplayGroupData)
      for (const workflowId of workflowIds) { // ident single workflowId within expected data
        const expectedData = expectedNonDisplayGroupData[`${workflowId}`]

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
        const expectedDataSorted = expectedData.sort()
        // get an array of filter ids for a given workflow id from the database
        expect(dbData).toEqual(expectedDataSorted)
      }
    }
    // Check exceptions
    if (expectedNumberOfExceptionRows) {
      const exceptionCount = await request.query(`
        select 
          count(*) 
        as 
          number 
        from 
          fff_staging.csv_staging_exception`)
      expect(exceptionCount.recordset[0].number).toBe(expectedNumberOfExceptionRows)
    }
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
})
