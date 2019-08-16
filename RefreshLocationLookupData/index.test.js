const fs = require('fs')
const fetch = require('node-fetch')
const message = require('../testing/mocks/defaultMessage')
const { pool, pooledConnect, sql } = require('../Shared/connection-pool')
const queueFunction = require('./index')
const STATUS_CODE_200 = 200
const STATUS_TEXT_OK = 'OK'
const TEXT_CSV = 'text/csv'

let request
let context
jest.mock('node-fetch')

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
  context = require('../testing/mocks/defaultContext')
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

describe('Refresh location lookup data', () => {
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

  it('should group locations by plot ID and workflow ID', async () => {
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
})

// TO DO - Add more test cases.

async function refreshLocationLookupDataAndCheckExpectedResults (mockResponseData, expectedLocationLookupData) {
  await mockFetchResponse(mockResponseData)
  await queueFunction(context, message)
  await checkExpectedResults(expectedLocationLookupData)
}
async function mockFetchResponse (mockResposeData) {
  const mockResponse = {
    status: mockResposeData.statusCode,
    body: fs.createReadStream(`testing/csv/${mockResposeData.filename}`),
    statusText: mockResposeData.statusText,
    headers: { 'Content-Type': mockResposeData.contentType },
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

  expect(result.recordset[0].number).toBe(expectedNumberOfRows)

  if (expectedNumberOfRows > 0) {
    // TO DO - Query the database and check that the locations associated with each grouping of workflow ID and plot ID are
    // as expected.
  }
}
