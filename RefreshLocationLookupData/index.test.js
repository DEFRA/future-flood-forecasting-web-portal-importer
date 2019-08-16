const fs = require('fs')
const fetch = require('node-fetch')
const message = require('../testing/mocks/defaultMessage')
const { pool, pooledConnect, sql } = require('../Shared/connection-pool')
const queueFunction = require('./index')
const STATUS_CODE_200 = 200
const STATUS_TEXT_OK = 'OK'

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

afterAll(() => {
  return pool.close()
})

describe('Refresh location lookup data', () => {
  it('should ignore an empty CSV file', async () => {
    const data = {
      statusCode: STATUS_CODE_200,
      filename: 'empty.csv',
      statusText: STATUS_TEXT_OK,
      contentType: 'text/csv'
    }
    await mockFetchResponse(data)
    await queueFunction(context, message)
    await checkExpectedNumberOfRecords(0)
  })
})

async function mockFetchResponse (data) {
  const mockResponse = {
    status: data.statusCode,
    body: fs.createReadStream(`testing/csv/${data.filename}`),
    statusText: data.statusText,
    headers: { 'Content-Type': data.contentType },
    sendAsJson: false
  }

  fetch.mockResolvedValue(mockResponse)
}

async function checkExpectedNumberOfRecords (expectedNumberOfRecords) {
  const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup`)
  expect(result.recordset[0].number).toBe(expectedNumberOfRecords)
}
