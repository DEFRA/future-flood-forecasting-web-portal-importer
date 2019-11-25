module.exports = describe('Refresh forecast location data tests', () => {
  const Context = require('../testing/mocks/defaultContext')
  const message = require('../testing/mocks/defaultMessage')
  const Connection = require('../Shared/connection-pool')
  const messageFunction = require('./index')
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

  const jestConnection = new Connection()
  const pool = jestConnection.pool
  const request = new sql.Request(pool)

  describe('The refresh forecast location data function:', () => {
    beforeAll(() => {
      return pool.connect()
    })

    beforeEach(() => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      return request.batch(`truncate table ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION`)
    })

    afterAll(() => {
      // Closing the DB connection allows Jest to exit successfully.
      return pool.close()
    })

    it('should ignore an empty CSV file', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'empty.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should ignore a CSV file with a valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should only load data rows that are complete within a csv that has some incomplete rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'some-data-rows-missing-values.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = [
        {
          Centre: 'Birmingham',
          MFDOArea: 'Derbyshire Nottinghamshire and Leicestershire',
          Catchemnt: 'Derwent',
          FFFSLocID: '4043',
          FFFSLocName: 'CHATSWORTH',
          PlotId: 'Fluvial_Gauge_MFDO',
          DRNOrder: 123
        }]

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should ignore a csv that has all rows with missing values', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'all-data-rows-missing-some-values.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should ignore rows that contains values exceeding a specified limit', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'one-row-has-data-over-specified-limits.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = [
        {
          Centre: 'Birmingham',
          MFDOArea: 'Derbyshire Nottinghamshire and Leicestershire',
          Catchemnt: 'Derwent',
          FFFSLocID: '4043',
          FFFSLocName: 'CHATSWORTH',
          PlotId: 'Fluvial_Gauge_MFDO',
          DRNOrder: 123
        }]

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should ignore a csv that has a string value in an integer field', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'string-not-integer.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should ignore a csv that has no header row, only data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should ignore a csv that has a missing header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should ignore a csv that has a misspelled header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'misspelled-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should not refresh when a non-csv file is supplied', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'json-file.json',
        statusText: STATUS_TEXT_OK,
        contentType: JSONFILE
      }

      const expectedForecastLocationData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should refresh given a valid CSV file', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = [{
        Centre: 'Birmingham',
        MFDOArea: 'Derbyshire Nottinghamshire and Leicestershire',
        Catchemnt: 'Derwent',
        FFFSLocID: 'Ashford+Chatsworth',
        FFFSLocName: 'Ashford+Chatsworth UG Derwent Derb to Wye confl',
        PlotId: 'Fluvial_Gauge_MFDO',
        DRNOrder: '123'
      },
      {
        Centre: 'Birmingham',
        MFDOArea: 'Derbyshire Nottinghamshire and Leicestershire',
        Catchemnt: 'Derwent',
        FFFSLocID: '4043',
        FFFSLocName: 'CHATSWORTH',
        PlotId: 'Fluvial_Gauge_MFDO',
        DRNOrder: '123'
      }]

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should not refresh given a valid CSV file with null values in some row cells', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'empty-values-in-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should not refresh if csv endpoint is not found(404)', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_404,
        statusText: STATUS_TEXT_NOT_FOUND,
        contentType: HTML,
        filename: '404-html.html'
      }

      const expectedLocationLookupData = []

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
    })

    it('should throw an exception when the csv server is unavailable', async () => {
      let expectedError = new Error(`connect ECONNREFUSED mockhost`)
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
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

      await lockForecastLocationTableAndCheckMessageCannotBeProcessed(mockResponseData)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)

    // End of describe
  })

  async function refreshForecastLocationDataAndCheckExpectedResults (mockResponseData, expectedForecastLocationData) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message) // calling actual function here
    await checkExpectedResults(expectedForecastLocationData)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/forecast_location_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedForecastLocationData) {
    const result = await request.query(`
       select count(*) 
       as number
       from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION
       `)
    let expectedNumberOfRows = expectedForecastLocationData.length

    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`database row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      // FFFSLOCID from expected data
      for (const row of expectedForecastLocationData) {
        let Centre = row.Centre
        let MFDOArea = row.MFDOArea
        let Catchment = row.Catchemnt
        let FFFSLocID = row.FFFSLocID
        let FFFSLocName = row.FFFSLocName
        let PlotId = row.PlotId
        let DRNOrder = row.DRNOrder

        const databaseResult = await request.query(`
      select 
       count(*) 
      as 
        number 
      from 
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION
      where 
        CENTRE = '${Centre}' and MFDO_AREA = '${MFDOArea}'
        and CATCHMENT = '${Catchment}' and FFFS_LOCATION_ID = '${FFFSLocID}' 
        and FFFS_LOCATION_NAME = '${FFFSLocName}' and FFFS_LOCATION_ID = '${FFFSLocID}'
      and PLOT_ID = '${PlotId}' and DRN_ORDER = '${DRNOrder}'
      `)
        expect(databaseResult.recordset[0].number).toEqual(1)
      }
    }
  }

  async function lockForecastLocationTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    try {
      // Lock the location lookup table and then try and process the message.
      transaction = new sql.Transaction(pool)
      await transaction.begin()
      const request = new sql.Request(transaction)
      await request.batch(`
        select
          *
        from
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION
        with
          (tablock, holdlock)
      `)
      await mockFetchResponse(mockResponseData)
      await messageFunction(context, message)
    } catch (err) {
      // Check that a request timeout occurs.
      expect(err.code).toTimeout(err.code)
    } finally {
      try {
        await transaction.rollback()
      } catch (err) { }
    }
  }
})
