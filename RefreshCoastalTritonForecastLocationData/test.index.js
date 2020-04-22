module.exports = describe('Refresh coastal location data tests', () => {
  const Context = require('../testing/mocks/defaultContext')
  const message = require('../testing/mocks/defaultMessage')
  const Connection = require('../Shared/connection-pool')
  const coastalRefreshFunction = require('./index')
  const fetch = require('node-fetch')
  const sql = require('mssql')
  const fs = require('fs')

  const JSONFILE = 'application/javascript'
  const STATUS_TEXT_NOT_FOUND = 'Not found'
  const STATUS_CODE_200 = 200
  const STATUS_CODE_404 = 404
  const STATUS_TEXT_OK = 'OK'
  const TEXT_CSV = 'text/csv'
  const HTML = 'html'
  jest.mock('node-fetch')

  let context
  let dummyData

  const jestConnection = new Connection()
  const pool = jestConnection.pool
  const request = new sql.Request(pool)

  describe('The refresh coastal triton forecast location data function:', () => {
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      dummyData = {
        FFFS_LOC_ID: 'dummy',
        FFFS_LOC_NAME: 'dummy',
        COASTAL_ORDER: 0,
        CENTRE: 'dummy',
        MFDO_AREA: 'dummy',
        TA_NAME: 'dummy',
        COASTAL_TYPE: 'Triton'
      }
      await request.query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception`)
      await request.query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_forecast_location`)
      await request.query(`insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_forecast_location (FFFS_LOC_ID, FFFS_LOC_NAME, COASTAL_ORDER, CENTRE, MFDO_AREA, TA_NAME, COASTAL_TYPE) values ('${dummyData.FFFS_LOC_ID}', '${dummyData.FFFS_LOC_NAME}', ${dummyData.COASTAL_ORDER}, '${dummyData.CENTRE}', '${dummyData.MFDO_AREA}', '${dummyData.TA_NAME}', '${dummyData.COASTAL_TYPE}')`)
    })

    afterAll(async () => {
      await request.query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_forecast_location`)
      await request.query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception`)
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

      const expectedCoastalLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 0
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
    })
    it('should refresh given a valid csv with 0 exceptions', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedCoastalLocationData = [
        {
          FFFS_LOC_ID: 'CV2',
          COASTAL_ORDER: 4000.000,
          CENTRE: 'Birmingham',
          MFDO_AREA: 'filler',
          TA_NAME: 'filler',
          COASTAL_TYPE: 'Triton'
        },
        {
          FFFS_LOC_ID: 'UKLFRAC',
          COASTAL_ORDER: 760.000,
          CENTRE: 'Birmingham',
          MFDO_AREA: 'filler',
          TA_NAME: 'filler',
          COASTAL_TYPE: 'Triton'
        }]
      const expectedNumberOfExceptionRows = 0
      await refreshCoastalLocationDataAndCheckExpectedResults(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows)
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
          COASTAL_ORDER: 8000.0,
          CENTRE: 'Birmingham',
          MFDO_AREA: 'MFDOAREA',
          TA_NAME: 'TANAME',
          COASTAL_TYPE: 'Triton'
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
    it('should not refresh if csv endpoint is not found(404)', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_404,
        statusText: STATUS_TEXT_NOT_FOUND,
        contentType: HTML,
        filename: '404-html.html'
      }

      const expectedForecastLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error(`No csv file detected`)
      await refreshCoastalLocationDataAndCheckFail(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows, expectedError)
    })
    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error(`connect ECONNREFUSED mockhost`)
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(coastalRefreshFunction(context, message)).rejects.toEqual(expectedError)
    })
    it('should throw an exception when the forecast location table is in use', async () => {
      // If the forecast location table is being refreshed messages are elgible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      await lockCoastalLocationTableAndCheckMessageCannotBeProcessed(mockResponseData)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    it('should throw an exception when a non-csv file is supplied', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'json-file.json',
        statusText: STATUS_TEXT_OK,
        contentType: JSONFILE
      }

      const expectedForecastLocationData = [dummyData]
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error(`No csv file detected`)
      await refreshCoastalLocationDataAndCheckFail(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows, expectedError)
    })
  })

  async function refreshCoastalLocationDataAndCheckExpectedResults (mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows) {
    await mockFetchResponse(mockResponseData)
    await coastalRefreshFunction(context, message) // calling actual function here
    await checkExpectedResults(expectedCoastalLocationData, expectedNumberOfExceptionRows)
  }

  // The following function is used in scenarios where a csv is successfully processed, but due to errors in the csv the app will then
  // attempt to overwrite and insert nothing into the database. This is caught and rejected in the function code (hence expecting this error/rejection).
  async function refreshCoastalLocationDataAndCheckFail (mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows, expectedError) {
    await mockFetchResponse(mockResponseData)
    await expect(coastalRefreshFunction(context, message)).rejects.toEqual(expectedError)
    await checkExpectedResults(expectedCoastalLocationData, expectedNumberOfExceptionRows)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/coastal_triton_forecast_location_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedCoastalLocationData, expectedNumberOfExceptionRows) {
    const coastalLocationCount = await request.query(`
       select count(*) 
       as number
       from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.COASTAL_FORECAST_LOCATION
       `)
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
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.COASTAL_FORECAST_LOCATION
      where 
      FFFS_LOC_ID = '${row.FFFS_LOC_ID}' and COASTAL_ORDER = ${row.COASTAL_ORDER} and 
      CENTRE = '${row.CENTRE}' and MFDO_AREA = '${row.MFDO_AREA}' and TA_NAME = '${row.TA_NAME}' and COASTAL_TYPE = '${row.COASTAL_TYPE}'
      `)
        expect(databaseResult.recordset[0].number).toEqual(1)
      }
    }
    // Check exceptions
    const exceptionCount = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception`)
    expect(exceptionCount.recordset[0].number).toBe(expectedNumberOfExceptionRows)
  }

  async function lockCoastalLocationTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.query(`
      insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_forecast_location (FFFS_LOC_ID, FFFS_LOC_NAME, COASTAL_ORDER, CENTRE, MFDO_AREA, TA_NAME, COASTAL_TYPE) values ('dummyData2', 'dummyData2', 2, 'dummyData2', 'dummyData2', 'dummyData2', 'Triton')
    `)
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
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception
    order by
      exception_time desc
  `)
    expect(result.recordset[0].description).toContain(expectedErrorDescription)
  }
})
