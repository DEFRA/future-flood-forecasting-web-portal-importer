const MVTRefreshFunction = require('../../../RefreshMVTData/index')
const ConnectionPool = require('../../../Shared/connection-pool')
const CommonWorkflowCSVTestUtils = require('../shared/common-workflow-csv-test-utils')
const Util = require('../shared/common-csv-refresh-utils')
const Context = require('../mocks/defaultContext')
const message = require('../mocks/defaultMessage')
const fetch = require('node-fetch')
const sql = require('mssql')
const fs = require('fs')

jest.mock('node-fetch')

module.exports = describe('Refresh mvt data tests', () => {
  const STATUS_CODE_200 = 200
  const STATUS_TEXT_OK = 'OK'
  const TEXT_CSV = 'text/csv'
  const HTML = 'html'

  let context
  let dummyData
  let commonCSVTestUtils
  let commonWorkflowCSVTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const request = new sql.Request(pool)

  describe('The refresh Multivariate Thresholds data function:', () => {
    const ORIGINAL_ENV = process.env
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.serviceConfigurationUpdateCompleted = []
      commonCSVTestUtils = new Util(context, pool)
      commonWorkflowCSVTestUtils = new CommonWorkflowCSVTestUtils(context, pool)
      dummyData = {
        CENTRE: 'dummy',
        CRITICAL_CONDITION_ID: 'dummy',
        INPUT_LOCATION_ID: 'dummy',
        OUTPUT_LOCATION_ID: 'dummy',
        TARGET_AREA_CODE: 'dummy',
        INPUT_PARAMETER_ID: 'dummy',
        LOWER_BOUND: 0,
        UPPER_BOUND: 0.5,
        LOWER_BOUND_INCLUSIVE: 0,
        UPPER_BOUND_INCLUSIVE: 1,
        PRIORITY: 9
      }
      await request.query('delete from fff_staging.csv_staging_exception')
      await request.query('delete from fff_staging.multivariate_thresholds')
      await request.query(`insert into fff_staging.multivariate_thresholds (CENTRE, CRITICAL_CONDITION_ID, INPUT_LOCATION_ID, OUTPUT_LOCATION_ID, TARGET_AREA_CODE, INPUT_PARAMETER_ID, LOWER_BOUND, UPPER_BOUND, LOWER_BOUND_INCLUSIVE, UPPER_BOUND_INCLUSIVE, PRIORITY) 
      values ('${dummyData.CENTRE}', '${dummyData.CRITICAL_CONDITION_ID}', '${dummyData.INPUT_LOCATION_ID}', '${dummyData.OUTPUT_LOCATION_ID}', '${dummyData.TARGET_AREA_CODE}', '${dummyData.INPUT_PARAMETER_ID}', '${dummyData.LOWER_BOUND}', '${dummyData.UPPER_BOUND}', '${dummyData.LOWER_BOUND_INCLUSIVE}', '${dummyData.UPPER_BOUND_INCLUSIVE}', '${dummyData.PRIORITY}')`)
      await request.query('delete from fff_staging.non_workflow_refresh')
      await request.query('delete from fff_staging.workflow_refresh')
    })

    afterEach(async () => {
      process.env = { ...ORIGINAL_ENV }
    })
    afterAll(async () => {
      await request.query('delete from fff_staging.multivariate_thresholds')
      await request.query('delete from fff_staging.csv_staging_exception')
      await request.query('delete from fff_staging.non_workflow_refresh')
      await request.query('delete from fff_staging.workflow_refresh')
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

      const expectedMVTData = [dummyData]
      const expectedNumberOfExceptionRows = 0
      await refreshMVTDataAndCheckExpectedResults(mockResponseData, expectedMVTData, expectedNumberOfExceptionRows)
    })
    it('should refresh given a valid csv, with 0 exceptions', async () => {
      process.env['AzureWebJobs.ProcessFewsEventCode.Disabled'] = 'true'
      process.env['AzureWebJobs.ImportFromFews.Disabled'] = 'true'

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedMVTData = [
        {
          CENTRE: 'CENTRE1',
          CRITICAL_CONDITION_ID: 'dummy',
          INPUT_LOCATION_ID: 'dummy',
          OUTPUT_LOCATION_ID: 'dummy',
          TARGET_AREA_CODE: 'dummy',
          INPUT_PARAMETER_ID: 'dummy',
          LOWER_BOUND: 0.1,
          UPPER_BOUND: 0.2,
          LOWER_BOUND_INCLUSIVE: 0,
          UPPER_BOUND_INCLUSIVE: 1,
          PRIORITY: 9
        },
        {
          CENTRE: 'CENTRE2',
          CRITICAL_CONDITION_ID: 'dummy',
          INPUT_LOCATION_ID: 'dummy',
          OUTPUT_LOCATION_ID: 'dummy',
          TARGET_AREA_CODE: 'dummy',
          INPUT_PARAMETER_ID: 'dummy',
          LOWER_BOUND: 0,
          UPPER_BOUND: 0,
          LOWER_BOUND_INCLUSIVE: 0,
          UPPER_BOUND_INCLUSIVE: 0,
          PRIORITY: 1
        }]
      const expectedNumberOfExceptionRows = 0
      await refreshMVTDataAndCheckExpectedResults(mockResponseData, expectedMVTData, expectedNumberOfExceptionRows)
    })
    it('should ignore a CSV file with a valid header row but no data rows', async () => {
      // Core engine message processing is enabled implicitly.
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-header-row-no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [dummyData]
      const expectedNumberOfExceptionRows = 0
      const expectedServiceConfigurationUpdateNotification = false
      const expectedErrorDescription = 'row is missing data'
      // Ensure a service configuration update is detected.
      await commonCSVTestUtils.insertNonWorkflowRefreshRecords()
      await commonWorkflowCSVTestUtils.insertWorkflowRefreshRecords()
      await refreshMVTDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData, expectedNumberOfExceptionRows, expectedErrorDescription, expectedServiceConfigurationUpdateNotification, expectedServiceConfigurationUpdateNotification)
    })
    it('should ignore rows that contains values exceeding a specified limit', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'one-row-with-data-over-specified-limits.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [{
        CENTRE: 'CENTRE1',
        CRITICAL_CONDITION_ID: 'dummy',
        INPUT_LOCATION_ID: 'dummy',
        OUTPUT_LOCATION_ID: 'dummy',
        TARGET_AREA_CODE: 'dummy',
        INPUT_PARAMETER_ID: 'dummy',
        LOWER_BOUND: 0.1,
        UPPER_BOUND: 0.2,
        LOWER_BOUND_INCLUSIVE: 0,
        UPPER_BOUND_INCLUSIVE: 1,
        PRIORITY: 5
      }]
      const expectedNumberOfExceptionRows = 1
      const expectedErrorDescription = 'String or binary data would be truncated'
      await refreshMVTDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData, expectedNumberOfExceptionRows, expectedErrorDescription)
    })
    it('should load rows that contain true/false or 0/1 for a bit column and ignore rows that contain not bit values', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'pass-fail-bit-value-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [{
        CENTRE: 'CENTRE1',
        CRITICAL_CONDITION_ID: 'dummy',
        INPUT_LOCATION_ID: 'dummy',
        OUTPUT_LOCATION_ID: 'dummy',
        TARGET_AREA_CODE: 'dummy',
        INPUT_PARAMETER_ID: 'dummy',
        LOWER_BOUND: 0.4,
        UPPER_BOUND: 0.3,
        LOWER_BOUND_INCLUSIVE: 0,
        UPPER_BOUND_INCLUSIVE: 1,
        PRIORITY: 6
      }]
      const expectedNumberOfExceptionRows = 1
      const expectedErrorDescription = 'Unexpected token o in JSON at position 1'
      await refreshMVTDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData, expectedNumberOfExceptionRows, expectedErrorDescription)
    })
    it('should ignore a csv that has no header row, only data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-data-rows-no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [dummyData]
      const expectedNumberOfExceptionRows = 2
      const expectedErrorDescription = 'row is missing data'
      await refreshMVTDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData, expectedNumberOfExceptionRows, expectedErrorDescription)
    })
    it('should ignore a csv that has a misspelled header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'headers-misspelled.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [dummyData]
      const expectedNumberOfExceptionRows = 3
      const expectedErrorDescription = 'row is missing data'
      await refreshMVTDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData, expectedNumberOfExceptionRows, expectedErrorDescription)
    })
    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error('connect ECONNREFUSED mockhost')
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(MVTRefreshFunction(context, message)).rejects.toEqual(expectedError)
    })
    it('should throw an exception when the multivariate threshold table is in use', async () => {
      // If the multivariate threshold table is being refreshed, messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      await lockMultivariateThresholdTableAndCheckMessageCannotBeProcessed(mockResponseData)
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

      const expectedData = [dummyData]
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error('No csv file detected')

      await expect(MVTRefreshFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData, expectedNumberOfExceptionRows)
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

      const expectedData = [dummyData]
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error('No csv file detected')

      await expect(MVTRefreshFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData, expectedNumberOfExceptionRows)
    })
    it('should refresh a valid csv containing empty values for decimal columns', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'empty-decimal-values.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedMVTData = [
        {
          CENTRE: 'CENTRE1',
          CRITICAL_CONDITION_ID: 'dummy',
          INPUT_LOCATION_ID: 'dummy',
          OUTPUT_LOCATION_ID: 'dummy',
          TARGET_AREA_CODE: 'dummy',
          INPUT_PARAMETER_ID: 'dummy',
          LOWER_BOUND: 0.1,
          UPPER_BOUND: null,
          LOWER_BOUND_INCLUSIVE: 0,
          UPPER_BOUND_INCLUSIVE: 1,
          PRIORITY: 9
        },
        {
          CENTRE: 'CENTRE2',
          CRITICAL_CONDITION_ID: 'dummy',
          INPUT_LOCATION_ID: 'dummy',
          OUTPUT_LOCATION_ID: 'dummy',
          TARGET_AREA_CODE: 'dummy',
          INPUT_PARAMETER_ID: 'dummy',
          LOWER_BOUND: null,
          UPPER_BOUND: 0,
          LOWER_BOUND_INCLUSIVE: 0,
          UPPER_BOUND_INCLUSIVE: 0,
          PRIORITY: 1
        }]
      await mockFetchResponse(mockResponseData)
      await MVTRefreshFunction(context, message)
      // we cannot check for null columns with comparison operators, such as =, <, or <>. Simply check the row count is correct.
      await checkResultCount(expectedMVTData.length)
    })
    it('should ignore an invalid csv and load the rows as exceptions. Following a successful csv load, all the old exceptions for that csv type should be removed', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'headers-misspelled.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [dummyData]
      const expectedNumberOfExceptionRows = 3
      const expectedErrorDescription = 'row is missing data'
      const mockResponseData2 = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedMVTData = [
        {
          CENTRE: 'CENTRE1',
          CRITICAL_CONDITION_ID: 'dummy',
          INPUT_LOCATION_ID: 'dummy',
          OUTPUT_LOCATION_ID: 'dummy',
          TARGET_AREA_CODE: 'dummy',
          INPUT_PARAMETER_ID: 'dummy',
          LOWER_BOUND: 0.1,
          UPPER_BOUND: 0.2,
          LOWER_BOUND_INCLUSIVE: 0,
          UPPER_BOUND_INCLUSIVE: 1,
          PRIORITY: 9
        },
        {
          CENTRE: 'CENTRE2',
          CRITICAL_CONDITION_ID: 'dummy',
          INPUT_LOCATION_ID: 'dummy',
          OUTPUT_LOCATION_ID: 'dummy',
          TARGET_AREA_CODE: 'dummy',
          INPUT_PARAMETER_ID: 'dummy',
          LOWER_BOUND: 0,
          UPPER_BOUND: 0,
          LOWER_BOUND_INCLUSIVE: 0,
          UPPER_BOUND_INCLUSIVE: 0,
          PRIORITY: 1
        }]
      const expectedNumberOfExceptionRows2 = 1
      const expectedErrorDescription2 = 'test data'
      await refreshMVTDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData, expectedNumberOfExceptionRows, expectedErrorDescription)
      await commonCSVTestUtils.insertCSVStagingException()
      await refreshMVTDataAndCheckExpectedResults(mockResponseData2, expectedMVTData, expectedNumberOfExceptionRows2, expectedErrorDescription2)
    })
    it('should refresh given a valid csv with string values set to NULL for upper bound and lower bound (inserting NULL values into the db via the preprocessor), with 0 exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-with-NaN-bound-values.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedMVTData = [
        {
          CENTRE: 'CENTRE1',
          CRITICAL_CONDITION_ID: 'dummy',
          INPUT_LOCATION_ID: 'dummy',
          OUTPUT_LOCATION_ID: 'dummy',
          TARGET_AREA_CODE: 'dummy',
          INPUT_PARAMETER_ID: 'dummy',
          LOWER_BOUND: 'NaN1',
          UPPER_BOUND: 'NaN2',
          LOWER_BOUND_INCLUSIVE: 0,
          UPPER_BOUND_INCLUSIVE: 1,
          PRIORITY: 9
        },
        {
          CENTRE: 'CENTRE2',
          CRITICAL_CONDITION_ID: 'dummy',
          INPUT_LOCATION_ID: 'dummy',
          OUTPUT_LOCATION_ID: 'dummy',
          TARGET_AREA_CODE: 'dummy',
          INPUT_PARAMETER_ID: 'dummy',
          LOWER_BOUND: 'NaN3',
          UPPER_BOUND: 'NaN4',
          LOWER_BOUND_INCLUSIVE: 0,
          UPPER_BOUND_INCLUSIVE: 0,
          PRIORITY: 1
        }]
      const expectedNumberOfExceptionRows = 0
      // We cannot check for a null value with a WHERE clause, therefore just check the row has successfully inserted into the table.
      const skipDetailedCheck = true
      const expectedErrorDescription = false
      await refreshMVTDataAndCheckExpectedResults(mockResponseData, expectedMVTData, expectedNumberOfExceptionRows, expectedErrorDescription, skipDetailedCheck)
    })
  })

  async function refreshMVTDataAndCheckExpectedResults (mockResponseData, expectedMVTData, expectedNumberOfExceptionRows, expectedErrorDescription, skipDetailedCheck, expectedServiceConfigurationUpdateNotification) {
    await mockFetchResponse(mockResponseData)
    await MVTRefreshFunction(context, message) // calling actual function here
    await checkExpectedResults(expectedMVTData, expectedNumberOfExceptionRows, expectedErrorDescription, skipDetailedCheck, expectedServiceConfigurationUpdateNotification)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/function-tests/RefreshMVTData/mvt_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false,
      url: '.csv'
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkResultCount (expectedNumberOfRows) {
    const MVTCount = await request.query(`
    select 
      count(*) 
    as 
      number
    from 
      fff_staging.multivariate_thresholds`)

    expect(MVTCount.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`Actual data row count: ${MVTCount.recordset[0].number}, test data row count: ${expectedNumberOfRows}`)
  }

  async function checkExpectedResults (expectedMVTData, expectedNumberOfExceptionRows, expectedErrorDescription, skipDetailedCheck, expectedServiceConfigurationUpdateNotification) {
    const expectedNumberOfRows = expectedMVTData.length
    await checkResultCount(expectedNumberOfRows)
    // Check each expected row is in the database
    if (expectedNumberOfRows > 0 && !skipDetailedCheck) {
      for (const row of expectedMVTData) {
        const databaseResult = await request.query(`
        select 
          count(*) 
        as 
         number 
        from 
         fff_staging.multivariate_thresholds
        where 
          CENTRE = '${row.CENTRE}' and CRITICAL_CONDITION_ID = '${row.CRITICAL_CONDITION_ID}' and INPUT_LOCATION_ID = '${row.INPUT_LOCATION_ID}' and OUTPUT_LOCATION_ID = '${row.OUTPUT_LOCATION_ID}' and TARGET_AREA_CODE = '${row.TARGET_AREA_CODE}' and INPUT_PARAMETER_ID = '${row.INPUT_PARAMETER_ID}' and LOWER_BOUND = '${row.LOWER_BOUND}' and UPPER_BOUND = '${row.UPPER_BOUND}' and LOWER_BOUND_INCLUSIVE = '${row.LOWER_BOUND_INCLUSIVE}' and UPPER_BOUND_INCLUSIVE = '${row.UPPER_BOUND_INCLUSIVE}' and PRIORITY = '${row.PRIORITY}'
      `)
        expect(databaseResult.recordset[0].number).toEqual(1)
      }
    }
    // Check exceptions (including a potential expectation of no exceptions)
    if (expectedNumberOfExceptionRows !== null) {
      const exceptionCount = await request.query(`
      select 
        count(*) 
      as 
        number 
      from 
        fff_staging.csv_staging_exception`)
      expect(exceptionCount.recordset[0].number).toBe(expectedNumberOfExceptionRows)
      if (expectedNumberOfExceptionRows > 0 && expectedErrorDescription) {
        await checkExceptionIsCorrect(expectedErrorDescription)
      }
    }

    // Check the expected service configuration update notification status.
    await commonCSVTestUtils.checkExpectedServiceConfigurationUpdateNotificationStatus(context, expectedServiceConfigurationUpdateNotification)
  }

  async function lockMultivariateThresholdTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.query(`insert into fff_staging.multivariate_thresholds (CENTRE, CRITICAL_CONDITION_ID, INPUT_LOCATION_ID, OUTPUT_LOCATION_ID, TARGET_AREA_CODE, INPUT_PARAMETER_ID, LOWER_BOUND, UPPER_BOUND, LOWER_BOUND_INCLUSIVE, UPPER_BOUND_INCLUSIVE, PRIORITY) values ('${dummyData.CENTRE}', '${dummyData.CRITICAL_CONDITION_ID}', '${dummyData.INPUT_LOCATION_ID}', '${dummyData.OUTPUT_LOCATION_ID}', '${dummyData.TARGET_AREA_CODE}', '${dummyData.INPUT_PARAMETER_ID}', '${dummyData.LOWER_BOUND}', '${dummyData.UPPER_BOUND}', '${dummyData.LOWER_BOUND_INCLUSIVE}', '${dummyData.UPPER_BOUND_INCLUSIVE}', '${dummyData.PRIORITY}')`)
      await mockFetchResponse(mockResponseData)
      await expect(MVTRefreshFunction(context, message)).rejects.toBeTimeoutError('multivariate_thresholds')
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
