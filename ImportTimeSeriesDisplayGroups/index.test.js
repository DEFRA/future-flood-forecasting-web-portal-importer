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
  return request.batch(`truncate table ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup`)
})

beforeAll(() => {
  return request.batch(`
    insert into
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup (workflow_id, plot_id, location_ids)
    values
      ('Test_Workflow1', 'Test Plot1', 'Test Location1')
  `)
})

beforeAll(() => {
  return request.batch(`
    insert into
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup (workflow_id, plot_id, location_ids)
    values
      ('Test_Workflow2', 'Test Plot2a', 'Test Location2a')
  `)
})

beforeAll(() => {
  return request.batch(`
    insert into
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup (workflow_id, plot_id, location_ids)
    values
      ('Test_Workflow2', 'Test Plot2b', 'Test Location2b')
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
  it('should import data for a single plot associated with an approved forecast', async () => {
    const mockResponse = {
      data: {
        key: 'Timeseries display groups data'
      }
    }
    await processMessageAndCheckImportedData('singlePlotApprovedForecast', [mockResponse])
  })
  it('should import data for multiple plots associated with an approved forecast', async () => {
    const mockResponses = [{
      data: {
        key: 'First plot timeseries display groups data'
      }
    },
    {
      data: {
        key: 'Second plot timeseries display groups data'
      }
    }]
    await processMessageAndCheckImportedData('multiplePlotApprovedForecast', mockResponses)
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
    await processMessageAndCheckImportedData('forecastApprovedManually', [mockResponse])
  })
  it('should create a staging exception for an unknown workflow', async () => {
    const unknownWorkflow = 'unknownWorkflow'
    const workflowId = taskRunCompleteMessages[unknownWorkflow].input.description.split(' ')[1]
    await processMessageAndCheckStagingExceptionIsCreated(unknownWorkflow, `Missing location_lookup data for ${workflowId}`)
  })
  it('should create a staging exception for an invalid message', async () => {
    await processMessageAndCheckStagingExceptionIsCreated('forecastWithoutApprovalStatus', 'Unable to extract task run approval status from message')
  })
  it('should throw an exception when the core engine PI server is unavailable', async () => {
    const mockResponse = new Error('connect ECONNREFUSED mockhost')
    await processMessageAndCheckExceptionIsThrown('singlePlotApprovedForecast', mockResponse)
  })
})

async function processMessage (messageKey, mockResponses) {
  if (mockResponses) {
    let mock = axios.get
    for (const mockResponse of mockResponses) {
      mock = mock.mockReturnValueOnce(mockResponse)
    }
  }
  await queueFunction(context, JSON.stringify(taskRunCompleteMessages[messageKey]))
}

async function processMessageAndCheckImportedData (messageKey, mockResponses) {
  await processMessage(messageKey, mockResponses)
  const receivedFewsData = []
  const result = await request.query(`
    select
      top(${mockResponses.length}) fews_data
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries
    order by
      start_time
  `)

  for (const index in result.recordset) {
    receivedFewsData.push(JSON.parse(result.recordset[index].fews_data))
  }

  for (const mockResponse of mockResponses) {
    expect(receivedFewsData).toContainEqual(mockResponse.data)
  }
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

async function processMessageAndCheckStagingExceptionIsCreated (messageKey, expectedErrorDescription) {
  await processMessage(messageKey)
  const result = await request.query(`
    select
      top(1) description
    from
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception
    order by
      exception_time desc
  `)
  expect(result.recordset[0].description).toBe(expectedErrorDescription)
}

async function processMessageAndCheckExceptionIsThrown (messageKey, mockErrorResponse) {
  axios.get.mockRejectedValue(mockErrorResponse)
  await expect(queueFunction(context, JSON.stringify(taskRunCompleteMessages[messageKey])))
    .rejects.toThrow(mockErrorResponse)
}
