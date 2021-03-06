const coastalRefreshFunction = require('../../../RefreshCoastalTritonForecastLocationData/index')
const CommonWorkflowCSVTestUtils = require('../shared/common-workflow-csv-test-utils')
const Util = require('../shared/common-csv-refresh-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const message = require('../mocks/defaultMessage')
const fetch = require('node-fetch')
const sql = require('mssql')
const fs = require('fs')

jest.mock('node-fetch')

module.exports = describe('Refresh coastal location data tests', () => {
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

  describe('The refresh coastal triton forecast location data function:', () => {
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
        FFFS_LOC_ID: 'dummy',
        FFFS_LOC_NAME: 'dummy',
        COASTAL_ORDER: 0,
        CENTRE: 'dummy',
        MFDO_AREA: 'dummy',
        TA_NAME: 'dummy',
        COASTAL_TYPE: 'Triton',
        LOCATION_X: 123456.123456,
        LOCATION_Y: 123456.123456
      }
      await request.query('delete from fff_staging.csv_staging_exception')
      await request.query('delete from fff_staging.coastal_forecast_location')
      await request.query(`insert into fff_staging.coastal_forecast_location (FFFS_LOC_ID, FFFS_LOC_NAME, COASTAL_ORDER, CENTRE, MFDO_AREA, TA_NAME, COASTAL_TYPE, LOCATION_X, LOCATION_Y) values ('${dummyData.FFFS_LOC_ID}', '${dummyData.FFFS_LOC_NAME}', ${dummyData.COASTAL_ORDER}, '${dummyData.CENTRE}', '${dummyData.MFDO_AREA}', '${dummyData.TA_NAME}', '${dummyData.COASTAL_TYPE}', ${dummyData.LOCATION_X}, ${dummyData.LOCATION_Y})`)
      await request.query('delete from fff_staging.non_workflow_refresh')
      await request.query('delete from fff_staging.workflow_refresh')
    })

    afterAll(async () => {
      await request.query('delete from fff_staging.coastal_forecast_location')
      await request.query('delete from fff_staging.csv_staging_exception')
      await request.query('delete from fff_staging.non_workflow_refresh')
      await request.query('delete from fff_staging.workflow_refresh')
      // Closing the DB connection allows Jest to exit successfully.
      await pool.close()
      process.env = { ...ORIGINAL_ENV }
    })
    it('should ignore an empty CSV file', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'empty.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedCoastalLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 0
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
    })
    it('should refresh given a valid csv with 0 exceptions', async () => {
      // Disable partial loading processing explicitly.
      process.env['AzureWebJobs.ImportFromFews.Disabled'] = 'true'

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedCoastalLocationData = [
        {
          FFFS_LOC_ID: 'CV2',
          COASTAL_ORDER: 56,
          CENTRE: 'Birmingham',
          MFDO_AREA: 'filler',
          TA_NAME: 'filler',
          COASTAL_TYPE: 'Triton',
          FFFS_LOC_NAME: 'name',
          LOCATION_X: 123456.123456,
          LOCATION_Y: 333333.123456
        },
        {
          FFFS_LOC_ID: 'ABVGTO',
          COASTAL_ORDER: 58,
          CENTRE: 'Birmingham',
          MFDO_AREA: 'filler',
          TA_NAME: 'filler',
          COASTAL_TYPE: 'Triton',
          FFFS_LOC_NAME: 'name',
          LOCATION_X: 123456.123456,
          LOCATION_Y: 123456.123456
        }]
      const expectedNumberOfExceptionRows = 0
      const expectedServiceConfigurationUpdateNotification = true
      // Ensure a service configuration update is detected.
      await commonCSVTestUtils.insertNonWorkflowRefreshRecords()
      await commonWorkflowCSVTestUtils.insertWorkflowRefreshRecords()
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification)
    })
    it('should ignore a csv file with a valid header but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }
      const expectedCoastalLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 0
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
    })
    it('should load complete rows into table and incomplete into exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'mixed-complete-incomplete-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedCoastalLocationData = [
        {
          FFFS_LOC_ID: 'CV2',
          COASTAL_ORDER: 70.0,
          CENTRE: 'Birmingham',
          MFDO_AREA: 'MFDOAREA',
          TA_NAME: 'TANAME',
          COASTAL_TYPE: 'Triton',
          FFFS_LOC_NAME: 'name',
          LOCATION_X: 123456.123456,
          LOCATION_Y: 123456.123456
        }]
      const expectedNumberOfExceptionRows = 1
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
    })
    it('should load a row with a invalid row data types into exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'invalid-data-type.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedCoastalLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 1
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
    })
    it('should load a row with an invalid coastal-type field into exceptions (violates sql CHECK constraint)', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'invalid-coastal-type-data.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedCoastalLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 1
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
    })
    it('should load a row with fields exceeding data limits into exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'exceeding-data-limit.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedCoastalLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 1
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
    })
    it('should load all rows in a csv that has no header into exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-header.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedCoastalLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 2
      const expectedExceptionDescription = 'row is missing data'
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
      await checkExceptionIsCorrect(expectedExceptionDescription)
    })
    it('should ignore a csv that has a mis-spelled header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'misspelled-header.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedCoastalLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 2
      const expectedExceptionDescription = 'row is missing data'
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
      await checkExceptionIsCorrect(expectedExceptionDescription)
    })
    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error('connect ECONNREFUSED mockhost')
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(coastalRefreshFunction(context, message)).rejects.toEqual(expectedError)
    })
    it('should throw an exception when the forecast location table is in use', async () => {
      // If the forecast location table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      await lockCoastalLocationTableAndCheckMessageCannotBeProcessed(mockResponseData)
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

      await expect(coastalRefreshFunction(context, message)).rejects.toEqual(expectedError)
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

      await expect(coastalRefreshFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData, expectedNumberOfExceptionRows)
    })
  })

  async function refreshCoastalLocationDataAndCheckExpectedResults (mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification) {
    await mockFetchResponse(mockResponseData)
    await coastalRefreshFunction(context, message) // calling actual function here
    await checkExpectedResults(expectedCoastalLocationData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/function-tests/RefreshCoastalTritonForecastLocationData/coastal_triton_forecast_location_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false,
      url: '.csv'
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedCoastalLocationData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification) {
    const coastalLocationCount = await request.query(`
    select 
      count(*) 
    as 
      number
    from 
      fff_staging.COASTAL_FORECAST_LOCATION`)
    const expectedNumberOfRows = expectedCoastalLocationData.length
    expect(coastalLocationCount.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`Actual data row count: ${coastalLocationCount.recordset[0].number}, test data row count: ${expectedNumberOfRows}`)
    // Check each expected row is in the database
    if (expectedNumberOfRows > 0) {
      for (const row of expectedCoastalLocationData) {
        const databaseResult = await request.query(`
        select 
          count(*) 
        as 
          number 
        from 
         fff_staging.COASTAL_FORECAST_LOCATION
        where 
          FFFS_LOC_ID = '${row.FFFS_LOC_ID}' and COASTAL_ORDER = ${row.COASTAL_ORDER} and 
          CENTRE = '${row.CENTRE}' and MFDO_AREA = '${row.MFDO_AREA}' and TA_NAME = '${row.TA_NAME}' and COASTAL_TYPE = '${row.COASTAL_TYPE}' and FFFS_LOC_NAME = '${row.FFFS_LOC_NAME}' and LOCATION_X = '${row.LOCATION_X}' and LOCATION_Y = '${row.LOCATION_Y}'`)
        expect(databaseResult.recordset[0].number).toEqual(1)
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

    // Check the expected service configuration update notification status.
    await commonCSVTestUtils.checkExpectedServiceConfigurationUpdateNotificationStatus(context, expectedServiceConfigurationUpdateNotification)
  }
  async function lockCoastalLocationTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.query(`
      insert into 
        fff_staging.coastal_forecast_location (FFFS_LOC_ID, FFFS_LOC_NAME, COASTAL_ORDER, CENTRE, MFDO_AREA, TA_NAME, COASTAL_TYPE, LOCATION_X, LOCATION_Y)
      values 
        ('dummyData2', 'dummyData2', 2, 'dummyData2', 'dummyData2', 'dummyData2', 'Triton', 123123, 123345)`
      )
      await mockFetchResponse(mockResponseData)
      await expect(coastalRefreshFunction(context, message)).rejects.toBeTimeoutError('coastal_forecast_location')
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
      exception_time desc`)
    expect(result.recordset[0].description).toContain(expectedErrorDescription)
  }
})
