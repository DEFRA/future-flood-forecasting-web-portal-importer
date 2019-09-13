const axios = require('axios')
const queueFunction = require('./index')
const taskRunCompleteMessages = require('../testing/messages/task-run-complete/messages')
const { pool, pooledConnect, sql } = require('../Shared/connection-pool')

let request
let context
jest.mock('axios')

beforeAll(() => {
  // Ensure the connection pool is ready
  return pooledConnect
})

beforeAll(() => {
  request = new sql.Request(pool)
  return request
})

beforeAll(() => {
  return request.batch(`
    insert into
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup (workflow_id, plot_id, location_ids)
    values
      ('Test_Workflow', 'Test Plot', 'Test Location')
  `)
})

beforeEach(() => {
  // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
  // function implementation for the function context needs creating for each test.
  context = require('../testing/mocks/defaultContext')
  return request.batch(`truncate table ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
})

afterAll(() => {
  return pool.close()
})

describe('Import timeseries display groups', () => {
  it('should import data for an approved forecast', async () => {
    const mockDataValue = 'Timeseries display groups data'
    const mockResponseData = {
      data: mockDataValue
    }

    axios.get.mockResolvedValue(mockResponseData)

    await queueFunction(context, JSON.stringify(taskRunCompleteMessages['approvedForecast']))
    const result = await request.query(`
      select
        -- Remove double quotes surrounding the value returned from the database.
        substring(fews_data, 2, len(fews_data) -2) as fews_data
      from
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries
    `)

    expect(result.recordset[0].fews_data).toBe(mockDataValue)
  })
})
