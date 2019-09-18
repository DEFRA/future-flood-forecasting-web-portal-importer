const axios = require('axios')
const Context = require('../testing/mocks/defaultContext')
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
  context = new Context()
  return request.batch(`truncate table ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
})

afterAll(() => {
  return pool.close()
})

describe('Message processing for task run completion', () => {
  it('should import data for an approved forecast', async () => {
    const mockResponse = {
      data: {
        key: 'Timeseries display groups data'
      }
    }
    await processMessageAndCheckImportedData('approvedForecast', mockResponse)
  })
  it('should not import data for an unapproved forecast', async () => {
    await processMessageAndCheckNoDataIsImported('unapprovedForecast')
  })
  it('should import data for a forecast approved manually', async () => {
    const mockResponse = {
      data: {
        key: 'Timeseries display groups data'
      }
    }
    await processMessageAndCheckImportedData('forecastApprovedManually', mockResponse)
  })
  it('should create a staging exception for an unknown workflow', async () => {
    await processMessageAndCheckStagingExeptionIsCreatedForWorkflow('unknownWorkflow')
  })
})

async function processMessage (messageKey, mockResponse) {
  context.log('Processing message')
  if (mockResponse) {
    axios.get.mockResolvedValue(mockResponse)
  }
  await queueFunction(context, JSON.stringify(taskRunCompleteMessages[messageKey]))
}

async function processMessageAndCheckImportedData (messageKey, mockResponse) {
  await processMessage(messageKey, mockResponse)

  const result = await request.query(`
    select
      top(1) fews_data
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries
    order by
      start_time desc
  `)

  expect(JSON.parse(result.recordset[0].fews_data)).toStrictEqual(mockResponse.data)
}

async function processMessageAndCheckNoDataIsImported (messageKey) {
  await processMessage(messageKey)
  const result = await request.query(`
    select
      count(*) as number
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries
  `)
  expect(result.recordset[0].number).toBe(0)
}

async function processMessageAndCheckStagingExeptionIsCreatedForWorkflow (messageKey) {
  const workflowId = taskRunCompleteMessages[messageKey].input.workflowId
  delete taskRunCompleteMessages[messageKey].input.workflowId
  await processMessage(messageKey)
  const result = await request.query(`
    select
      top(1) description
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception
    order by
      exception_time desc
  `)
  expect(result.recordset[0].description).toBe(`Missing location_lookup data for ${workflowId}`)
}
