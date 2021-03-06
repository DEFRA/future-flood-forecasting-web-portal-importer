const CommonWorkflowCsvTestUtils = require('../shared/common-workflow-csv-test-utils')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const { doInTransaction } = require('../../../Shared/transaction-helper')
const message = require('../mocks/defaultMessage')
const messageFunction = require('../../../RefreshIgnoredWorkflowData/index')
const fetch = require('node-fetch')
const sql = require('mssql')
const fs = require('fs')

jest.mock('node-fetch')

module.exports = describe('Ignored workflow loader tests', () => {
  const STATUS_CODE_200 = 200
  const STATUS_TEXT_OK = 'OK'
  const TEXT_CSV = 'text/csv'
  const HTML = 'html'

  let commonWorkflowCsvTestUtils
  let context
  let dummyData

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const request = new sql.Request(pool)

  describe('The refresh ignored workflow data function:', () => {
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      const config = {
        csvType: 'I'
      }

      commonWorkflowCsvTestUtils = new CommonWorkflowCsvTestUtils(context, pool, config)
      dummyData = [{ WorkflowId: 'dummyData' }]
      await request.batch('delete from fff_staging.csv_staging_exception')
      await request.batch('delete from fff_staging.staging_exception')
      await request.batch('delete from fff_staging.timeseries_staging_exception')
      await request.batch('delete from fff_staging.ignored_workflow')
      await request.batch('delete from fff_staging.workflow_refresh')
      await request.batch('insert into fff_staging.ignored_workflow (workflow_id) values (\'dummyData\')')
    })

    afterAll(async () => {
      await request.batch('delete from fff_staging.ignored_workflow')
      await request.batch('delete from fff_staging.csv_staging_exception')
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

      const expectedData = {
        ignoredWorkflowData: dummyData
      }

      await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should ignore a CSV file with a valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-header-row-no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        ignoredWorkflowData: dummyData
      }

      await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should ignore rows that contains values exceeding a specified limit', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'one-row-has-data-over-specified-limits.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [{
        WorkflowId: 'workflow787'
      }]

      const expectedData = {
        ignoredWorkflowData: expectedIgnoredWorkflowData,
        numberOfExceptionRows: 1
      }

      await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should ignore a csv that has no header row, only data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-data-rows-no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        ignoredWorkflowData: dummyData,
        numberOfExceptionRows: 1
      }

      await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should ignore a csv that has a misspelled header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'headers-misspelled.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedData = {
        ignoredWorkflowData: dummyData,
        numberOfExceptionRows: 1
      }

      await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedData)
    })
    it('should refresh given a valid CSV file and replay eligible failed messages', async () => {
      const expectWorkflowRefresh = true
      // Ensure messages linked to CSV associated staging exceptions/timeseries staging exceptions are replayed.
      await doInTransaction({ fn: insertExceptions, context, errorMessage: 'Error' })
      await loadValidCsvAndCheckExpectedResults(['ukeafffsmc00:000000001 message'], expectWorkflowRefresh)
    })
    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error('connect ECONNREFUSED mockhost')
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
    })
    it('should throw an exception when the ignored workflow table is in use', async () => {
      // If the ignored workflow table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-ignored-workflows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      await lockIgnoredWorkflowTableAndCheckMessageCannotBeProcessed(mockResponseData)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 5000)
    it('should load unloadable rows into csv exceptions table', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'invalid-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedErrorDescription = 'row is missing data.'

      await refreshIgnoredWorkflowDataAndCheckExceptionIsCreated(mockResponseData, expectedErrorDescription)
    })
    it('should not refresh when a non-csv file (JSON) is provided', async () => {
      const mockResponse = {
        status: STATUS_CODE_200,
        body: fs.createReadStream('testing/function-tests/general-files/json.json'),
        statusText: STATUS_TEXT_OK,
        headers: { 'Content-Type': 'application/javascript' },
        url: '.json'
      }
      await fetch.mockResolvedValue(mockResponse)

      const expectedData = {
        ignoredWorkflowData: dummyData,
        numberOfExceptionRows: 0
      }
      const expectedError = new Error('No csv file detected')

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData)
    })
    it('should not refresh if csv endpoint is not found(404)', async () => {
      const mockResponse = {
        status: 404,
        body: fs.createReadStream('testing/function-tests/general-files/404.html'),
        statusText: 'Not found',
        headers: { 'Content-Type': HTML },
        url: '.html'
      }
      await fetch.mockResolvedValue(mockResponse)

      const expectedData = {
        ignoredWorkflowData: dummyData,
        numberOfExceptionRows: 0
      }

      const expectedError = new Error('No csv file detected')

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData)
    })
    it('should allow optional use of a HTTP Authorization header ', async () => {
      process.env.CONFIG_AUTHORIZATION = 'Mock token'
      const expectWorkflowRefresh = true
      await loadValidCsvAndCheckExpectedResults(false, expectWorkflowRefresh)
    })
  })

  async function refreshIgnoredWorkflowDataAndCheckExpectedResults (mockResponseData, expectedData, expectWorkflowRefresh) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message) // calling actual function here
    await checkExpectedResults(expectedData, expectWorkflowRefresh)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/function-tests/RefreshIgnoredWorkflowData/ignored_workflow_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false,
      url: '.csv'
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedData, expectWorkflowRefresh) {
    const result = await request.query(`
    select 
      count(*)
    as 
      number
    from 
      fff_staging.ignored_workflow`)
    const expectedNumberOfRows = expectedData.ignoredWorkflowData.length

    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`Live data row count: ${result.recordset[0].number}, test data row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      for (const row of expectedData.ignoredWorkflowData) {
        const WorkflowId = row.WorkflowId

        const databaseResult = await request.query(`
        select
         count(*)
        as
          number
        from
          fff_staging.ignored_workflow
        where 
          workflow_id = '${WorkflowId}'
        `)
        expect(databaseResult.recordset[0].number).toEqual(1)
      }

      if (typeof expectWorkflowRefresh !== 'undefined') {
        // If the CSV table is expected to contain rows other than the row of dummy data check that the workflow refresh table
        // contains a row for the CSV.
        await commonWorkflowCsvTestUtils.checkWorkflowRefreshData(expectWorkflowRefresh)
      }
    }
    // Check exceptions
    const exceptionCount = await request.query(`
      select 
        count(*) 
      as 
        number 
      from 
        fff_staging.csv_staging_exception`)

    expect(exceptionCount.recordset[0].number).toBe(expectedData.numberOfExceptionRows || 0)

    // Check messages to be replayed
    await commonWorkflowCsvTestUtils.checkReplayedStagingExceptionMessages(expectedData.replayedStagingExceptionMessages)
    await commonWorkflowCsvTestUtils.checkReplayedTimeseriesStagingExceptionMessages(expectedData.replayedTimeseriesStagingExceptionMessages)
  }

  async function lockIgnoredWorkflowTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    const tableName = 'ignored_workflow'
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(`
        insert into 
          fff_staging.${tableName} (WORKFLOW_ID)
        values 
          ('ignored_1')
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

  async function refreshIgnoredWorkflowDataAndCheckExceptionIsCreated (mockResponseData, expectedErrorDescription) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message) // This is a call to the function index
    const result = await request.query(`
    select
      top(1) description
    from
      fff_staging.csv_staging_exception
    order by
      exception_time desc
  `)
    expect(result.recordset[0].description).toBe(expectedErrorDescription)
  }

  async function loadValidCsvAndCheckExpectedResults (replayedStagingExceptionMessages, expectWorkflowRefresh) {
    const mockResponseData = {
      statusCode: STATUS_CODE_200,
      filename: 'valid-ignored-workflows.csv',
      statusText: STATUS_TEXT_OK,
      contentType: TEXT_CSV
    }

    const expectedIgnoredWorkflowData = [{
      WorkflowId: 'workflow1'
    },
    {
      WorkflowId: 'workflow2'
    },
    {
      WorkflowId: 'workflow3'
    }]

    const expectedData = {
      ignoredWorkflowData: expectedIgnoredWorkflowData,
      replayedStagingExceptionMessages: replayedStagingExceptionMessages || []
    }

    await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedData, expectWorkflowRefresh)
  }

  async function insertExceptions (transaction, context) {
    await new sql.Request(transaction).batch(`
      insert into
        fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
      values
        ('ukeafffsmc00:000000001 message', 'Missing PI Server input data for workflow1', 'ukeafffsmc00:000000001', 'P', 'workflow1', getutcdate());

      insert into
        fff_staging.staging_exception (payload, description, task_run_id, source_function, workflow_id, exception_time)
      values
        ('ukeafffsmc00:000000002 message', 'Missing PI Server input data for Missing Workflow', 'ukeafffsmc00:000000002', 'P', 'Missing Workflow', getutcdate());
    `)
  }
})
