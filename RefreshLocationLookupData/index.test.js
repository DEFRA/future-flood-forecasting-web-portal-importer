const fs = require('fs')
const fetch = require('node-fetch')
const Context = require('../testing/mocks/defaultContext')
const message = require('../testing/mocks/defaultMessage')
const { pool, pooledConnect, sql } = require('../Shared/connection-pool')
const messageFunction = require('./index')
const STATUS_CODE_200 = 200
const STATUS_CODE_404 = 404
const STATUS_TEXT_OK = 'OK'
const STATUS_TEXT_NOT_FOUND = 'Not found'
const TEXT_CSV = 'text/csv'
const HTML = 'html'

let request
let context
jest.mock('node-fetch')

if (process.env['TEST_TIMEOUT']) {
  jest.setTimeout(parseInt(process.env['TEST_TIMEOUT']))
}

describe('The refresh location lookup data function:', () => {
  beforeAll(() => {
    // Ensure the connection pool is ready
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
    return request.batch(`truncate table ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup`)
  })

  afterEach(() => {
    // As the connection pool is only closed at the end of the test suite the global temporary table used by each function
    // invocation needs to be dropped manually between each test case.
    return request.batch(`drop table if exists ##location_lookup_temp`)
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

    const expectedLocationLookupData = {}

    await refreshLocationLookupDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
  })

  it('should group locations by plot ID and workflow ID given single location per workflowId/plotId', async () => {
    const mockResponseData = {
      statusCode: STATUS_CODE_200,
      filename: 'single-location-per-plot-for-workflow.csv',
      statusText: STATUS_TEXT_OK,
      contentType: TEXT_CSV
    }

    const expectedLocationLookupData = {
      workflow1: {
        plot1: ['location4'],
        plot2: ['location1']
      },
      workflow2: {
        plot1: ['location1']
      }
    }

    await refreshLocationLookupDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
  })

  it('should group locations by plot ID and workflow ID given multiple combinations of workflowId and plotId', async () => {
    const mockResponseData = {
      statusCode: STATUS_CODE_200,
      filename: 'multiple-locations-per-plot-for-workflow.csv',
      statusText: STATUS_TEXT_OK,
      contentType: TEXT_CSV
    }

    const expectedLocationLookupData = {
      workflow1: {
        plot1: ['location1', 'location2', 'location3', 'location4'],
        plot2: ['location1']
      },
      workflow2: {
        plot1: ['location1', 'location2']
      }
    }

    await refreshLocationLookupDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
  })

  it('should not refresh with valid header row but no data rows', async () => {
    const mockResponseData = {
      statusCode: STATUS_CODE_200,
      filename: 'valid-header-row-no-data-rows.csv',
      statusText: STATUS_TEXT_OK,
      contentType: TEXT_CSV
    }

    const expectedLocationLookupData = {
    }

    await refreshLocationLookupDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
  })

  it('should reject insert if there is no header row, expect the first row to be treated as the header', async () => {
    const mockResponseData = {
      statusCode: STATUS_CODE_200,
      filename: 'valid-data-rows-no-header-row.csv',
      statusText: STATUS_TEXT_OK,
      contentType: TEXT_CSV
    }

    const expectedLocationLookupData = {
    }

    await refreshLocationLookupDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
  })

  it('should ommit rows with missing values in columns', async () => {
    const mockResponseData = {
      statusCode: STATUS_CODE_200,
      filename: 'missing-data-in-columns.csv',
      statusText: STATUS_TEXT_OK,
      contentType: TEXT_CSV
    }

    const expectedLocationLookupData = {
      workflow2: {
        plot1: ['location1']
      }
    }

    await refreshLocationLookupDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
  })

  it('should ommit rows with missing values in entire column', async () => {
    const mockResponseData = {
      statusCode: STATUS_CODE_200,
      filename: 'missing-data-in-entire-column.csv',
      statusText: STATUS_TEXT_OK,
      contentType: TEXT_CSV
    }

    const expectedLocationLookupData = {
    }

    await refreshLocationLookupDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
  })

  it('should not refresh when a non-csv file (JSON) is provided', async () => {
    const mockResponseData = {
      statusCode: STATUS_CODE_200,
      filename: 'json-file.json',
      statusText: STATUS_TEXT_OK,
      contentType: TEXT_CSV
    }

    const expectedLocationLookupData = {
    }

    await refreshLocationLookupDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
  })

  it('should not refresh if csv endpoint is not found(404)', async () => {
    const mockResponseData = {
      statusCode: STATUS_CODE_404,
      statusText: STATUS_TEXT_NOT_FOUND,
      contentType: HTML,
      filename: '404-html.html'
    }

    const expectedLocationLookupData = {
    }

    await refreshLocationLookupDataAndCheckExpectedResults(mockResponseData, expectedLocationLookupData)
  })

  it('should throw an exception when the csv server is unavailable', async () => {
    let expectedError = new Error(`connect ECONNREFUSED mockhost`)
    fetch.mockImplementation(() => {
      throw new Error('connect ECONNREFUSED mockhost')
    })
    await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
  })

  it('should throw an exception when the location lookup table is being used', async () => {
    // If the location lookup table is being refreshed messages are elgible for replay a certain number of times
    // so check that an exception is thrown to facilitate this process.

    const mockResponseData = {
      statusCode: STATUS_CODE_200,
      filename: 'multiple-locations-per-plot-for-workflow.csv',
      statusText: STATUS_TEXT_OK,
      contentType: TEXT_CSV
    }

    await lockLocationLookupTableAndCheckMessageCannotBeProcessed(mockResponseData)
    // Set the test timeout higher than the database request timeout.
  }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)

  // End of describe
})

async function refreshLocationLookupDataAndCheckExpectedResults (mockResponseData, expectedLocationLookupData) {
  await mockFetchResponse(mockResponseData)
  await messageFunction(context, message) // calling actual function here
  await checkExpectedResults(expectedLocationLookupData)
}

async function mockFetchResponse (mockResponseData) {
  let mockResponse = {}
  mockResponse = {
    status: mockResponseData.statusCode,
    body: fs.createReadStream(`testing/csv/${mockResponseData.filename}`),
    statusText: mockResponseData.statusText,
    headers: { 'Content-Type': mockResponseData.contentType },
    sendAsJson: false
  }
  fetch.mockResolvedValue(mockResponse)
}

async function checkExpectedResults (expectedLocationLookupData) {
  const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup`)
  const workflowIds = Object.keys(expectedLocationLookupData)
  let expectedNumberOfRows = 0

  // The number of rows returned from the database should be equal to the sum of plot ID elements nested within
  // all workflow ID elements of the expected location lookup data.
  for (const workflowId of workflowIds) {
    expectedNumberOfRows += Object.keys(expectedLocationLookupData[workflowId]).length
  }

  // Query the database and check that the locations associated with each grouping of workflow ID and plot ID areas expected.
  expect(result.recordset[0].number).toBe(expectedNumberOfRows)
  context.log(`databse row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

  if (expectedNumberOfRows > 0) {
    const workflowIds = Object.keys(expectedLocationLookupData)
    for (const workflowId of workflowIds) { // ident single workflowId within expected data
      const plotIds = expectedLocationLookupData[`${workflowId}`] // ident group of plot ids for workflowId
      for (const plotId in plotIds) { // ident single plot id within workflowId to access locations
        // expected data layout
        const locationIds = plotIds[`${plotId}`] // ident group of location ids for single plotid and single workflowid combination
        const expectedLocationsArray = locationIds.sort()

        // actual db data
        const locationQuery = await request.query(`
        SELECT *
        FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup
        WHERE workflow_id = '${workflowId}' AND plot_id = '${plotId}'
        `)
        const rows = locationQuery.recordset
        const dbLocationsResult = rows[0].LOCATION_IDS
        const dbLocations = dbLocationsResult.split(';').sort()
        expect(dbLocations).toEqual(expectedLocationsArray)
      }
    }
  }
}

async function lockLocationLookupTableAndCheckMessageCannotBeProcessed (mockResponseData) {
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
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup
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
