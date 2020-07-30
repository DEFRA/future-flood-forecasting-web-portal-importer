module.exports = describe('Refresh forecast location data tests', () => {
  const Context = require('../mocks/defaultContext')
  const message = require('../mocks/defaultMessage')
  const ConnectionPool = require('../../../Shared/connection-pool')
  const messageFunction = require('../../../RefreshFluvialForecastLocationData/index')
  const fetch = require('node-fetch')
  const sql = require('mssql')
  const fs = require('fs')

  const STATUS_CODE_200 = 200
  const STATUS_TEXT_OK = 'OK'
  const TEXT_CSV = 'text/csv'
  const HTML = 'html'
  jest.mock('node-fetch')

  let context
  let dummyData

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const request = new sql.Request(pool)

  describe('The refresh forecast location data function:', () => {
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      dummyData = [{ Centre: 'dummyData', MFDOArea: 'dummyData', Catchment: 'dummyData', FFFSLocID: 'dummyData', FFFSLocName: 'dummyData', PlotId: 'dummyData', DRNOrder: 123, Order: 8888, Datum: 'mALD', CatchmentOrder: 2 }]
      await request.batch(`delete from fff_staging.csv_staging_exception`)
      await request.batch(`delete from fff_staging.fluvial_forecast_location`)
      await request.batch(`
      insert 
        into fff_staging.fluvial_forecast_location
        (CENTRE, MFDO_AREA, CATCHMENT, CATCHMENT_ORDER, FFFS_LOCATION_ID, FFFS_LOCATION_NAME, PLOT_ID, DRN_ORDER, DISPLAY_ORDER, DATUM) 
      values 
        ('dummyData', 'dummyData', 'dummyData', 2, 'dummyData', 'dummyData', 'dummyData', 123, 8888, 'mALD')
      `)
    })

    afterAll(async () => {
      await request.batch(`delete from fff_staging.fluvial_forecast_location`)
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

      const expectedForecastLocationData = dummyData
      const expectedNumberOfExceptionRows = 0
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
    })
    it('should ignore a CSV file with a valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = dummyData
      const expectedNumberOfExceptionRows = 0
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
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
          Catchment: 'Derwent',
          FFFSLocID: '40443',
          FFFSLocName: 'CHATSWORTH',
          PlotId: 'Fluvial_Gauge_MFDO',
          DRNOrder: 123,
          Order: 8988,
          Datum: 'mALD',
          CatchmentOrder: 1
        }]
      const expectedNumberOfExceptionRows = 1
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has all rows with missing values', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'all-data-rows-missing-some-values.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
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
          Catchment: 'Derwent',
          FFFSLocID: '4043',
          FFFSLocName: 'CHATSWORTH',
          PlotId: 'Fluvial_Gauge_MFDO',
          DRNOrder: 123,
          Order: 8888,
          Datum: 'mALD',
          CatchmentOrder: 1
        }]
      const expectedNumberOfExceptionRows = 0
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has a string value in an integer field', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'string-not-integer.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has no header row, only data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = dummyData
      const expectedNumberOfExceptionRows = 1
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has a missing header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has a misspelled header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'misspelled-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
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
        Catchment: 'Derwent',
        FFFSLocID: 'Ashford+Chatsworth',
        FFFSLocName: 'Ashford+Chatsworth UG Derwent Derb to Wye confl',
        PlotId: 'Fluvial_Gauge_MFDO',
        DRNOrder: 123,
        Order: 8888,
        Datum: 'mALD',
        CatchmentOrder: 1
      },
      {
        Centre: 'Birmingham',
        MFDOArea: 'Derbyshire Nottinghamshire and Leicestershire',
        Catchment: 'Derwent',
        FFFSLocID: '40443',
        FFFSLocName: 'CHATSWORTH',
        PlotId: 'Fluvial_Gauge_MFDO',
        DRNOrder: 123,
        Order: 8988,
        Datum: 'mALD',
        CatchmentOrder: 1
      }]
      const expectedNumberOfExceptionRows = 0
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
    })

    it('should not refresh given a valid CSV file with null values in some of all row cells', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'empty-values-in-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedForecastLocationData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows)
    })
    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error(`connect ECONNREFUSED mockhost`)
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
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

      await lockForecastLocationTableAndCheckMessageCannotBeProcessed(mockResponseData)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    it('should not refresh when a non-csv file (JSON) is provided', async () => {
      const mockResponse = {
        status: STATUS_CODE_200,
        body: fs.createReadStream(`testing/function-tests/general-files/json.json`),
        statusText: STATUS_TEXT_OK,
        headers: { 'Content-Type': 'application/javascript' },
        url: '.json'
      }
      await fetch.mockResolvedValue(mockResponse)

      const expectedData = dummyData
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error(`No csv file detected`)

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData, expectedNumberOfExceptionRows)
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
  })

  async function refreshForecastLocationDataAndCheckExpectedResults (mockResponseData, expectedForecastLocationData, expectedNumberOfExceptionRows) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message) // calling actual function here
    await checkExpectedResults(expectedForecastLocationData, expectedNumberOfExceptionRows)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/function-tests/RefreshFluvialForecastLocationData/fluvial_forecast_location_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false,
      url: '.csv'
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedForecastLocationData, expectedNumberOfExceptionRows) {
    const result = await request.query(`
    select 
      count(*) 
    as 
      number
    from 
      fff_staging.fluvial_forecast_location
       `)
    const expectedNumberOfRows = expectedForecastLocationData.length

    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`Live data row count: ${result.recordset[0].number}, test data row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      for (const row of expectedForecastLocationData) {
        const Centre = row.Centre
        const MFDOArea = row.MFDOArea
        const Catchment = row.Catchment
        const FFFSLocID = row.FFFSLocID
        const FFFSLocName = row.FFFSLocName
        const PlotId = row.PlotId
        const DRNOrder = row.DRNOrder
        const displayOrder = row.Order
        const catchmentOrder = row.CatchmentOrder

        const databaseResult = await request.query(`
      select 
        count(*) 
      as 
        number 
      from 
        fff_staging.fluvial_forecast_location
      where 
        CENTRE = '${Centre}' and MFDO_AREA = '${MFDOArea}'
        and CATCHMENT = '${Catchment}' and FFFS_LOCATION_ID = '${FFFSLocID}' and CATCHMENT_ORDER = '${catchmentOrder}'
        and FFFS_LOCATION_NAME = '${FFFSLocName}' and FFFS_LOCATION_ID = '${FFFSLocID}'
      and PLOT_ID = '${PlotId}' and DRN_ORDER = '${DRNOrder}' and DISPLAY_ORDER = '${displayOrder}'
      `)
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
  }

  async function lockForecastLocationTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    const tableName = 'fluvial_forecast_location'
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(`
      insert into 
        fff_staging.${tableName} (CENTRE, MFDO_AREA, CATCHMENT, FFFS_LOCATION_ID, FFFS_LOCATION_NAME, PLOT_ID, DRN_ORDER, DISPLAY_ORDER, DATUM, CATCHMENT_ORDER) 
      values 
        ('centre', 'mfdo_area', 'catchement', 'loc_id', 'locname', 'plotid', 123, 0, 'mALD', 5)
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
})
