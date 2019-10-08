module.exports = describe('Refresh forecast location data tests', () => {
  const fs = require('fs')
  const fetch = require('node-fetch')
  const Context = require('../testing/mocks/defaultContext')
  const message = require('../testing/mocks/defaultMessage')
  const { pooledConnect, pool, sql } = require('../Shared/connection-pool')
  const messageFunction = require('./index')
  const STATUS_CODE_200 = 200
  // const STATUS_CODE_404 = 404
  const STATUS_TEXT_OK = 'OK'
  // const STATUS_TEXT_NOT_FOUND = 'Not found'
  const TEXT_CSV = 'text/csv'
  // const HTML = 'html'

  let request
  let context
  jest.mock('node-fetch')

  describe('The refresh forecast location data function:', () => {
    beforeAll(() => {
      return pooledConnect
    })

    beforeAll(() => {
      request = new sql.Request(pool)
      return request
    })

    beforeEach(() => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      // The SQL TRUNCATE TABLE statement is used to remove all records from a table

      context = new Context()
      return request.batch(`truncate table ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION`)
    })

    afterEach(() => {
    })

    afterAll(() => {
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
        PlotID: 'Fluvial_Gauge_MFDO'
      },
      {
        Centre: 'Birmingham',
        MFDOArea: 'Derbyshire Nottinghamshire and Leicestershire',
        Catchemnt: 'Derwent',
        FFFSLocID: '4043',
        FFFSLocName: 'CHATSWORTH',
        PlotID: 'Fluvial_Gauge_MFDO'
      }]

      await refreshForecastLocationDataAndCheckExpectedResults(mockResponseData, expectedForecastLocationData)
    })

    it('should throw an exception when the csv server is unavailable', async () => {
      let expectedError = new Error(`connect ECONNREFUSED mockhost`)
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
    })

    it('should throw an exception when the forecast location table is being used', async () => {
      // If the location lookup table is being refreshed messages are elgible for replay a certain number of times
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
    const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION`)
    const expectedNumberOfRows = expectedForecastLocationData.length

    // The number of rows returned from the database should be equal to the sum of plot ID elements nested within
    // all workflow ID elements of the expected location lookup data.
    // for (const workflowId of workflowIds) {
    //   expectedNumberOfRows += Object.keys(expectedForecastLocationData[workflowId]).length
    // }

    // Query the database and check that the locations associated with each grouping of workflow ID and plot ID areas expected.
    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`databse row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

    // if (expectedNumberOfRows > 0) {
    //   const workflowIds = Object.keys(expectedForecastLocationData)
    //   for (const workflowId of workflowIds) { // ident single workflowId within expected data
    //     const plotIds = expectedForecastLocationData[`${workflowId}`] // ident group of plot ids for workflowId
    //     for (const plotId in plotIds) { // ident single plot id within workflowId to access locations
    //       // expected data layout
    //       const locationIds = plotIds[`${plotId}`] // ident group of location ids for single plotid and single workflowid combination
    //       const expectedLocationsArray = locationIds.sort()

    //       // actual db data
    //       const locationQuery = await request.query(`
    //       SELECT *
    //       FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION
    //       WHERE workflow_id = '${workflowId}' AND plot_id = '${plotId}'
    //       `)
    //       const rows = locationQuery.recordset
    //       const dbLocationsResult = rows[0].LOCATION_IDS
    //       const dbLocations = dbLocationsResult.split(';').sort()
    //       expect(dbLocations).toEqual(expectedLocationsArray)
    //     }
    //   }
    // }
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
      expect(err.code).toBe('EREQUEST')
    } finally {
      try {
        await transaction.rollback()
      } catch (err) { }
    }
  }
})
