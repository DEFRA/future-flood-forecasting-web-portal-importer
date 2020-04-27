module.exports = describe('Ignored workflow loader tests', () => {
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
  let dummyData

  const jestConnection = new Connection()
  const pool = jestConnection.pool
  const request = new sql.Request(pool)

  describe('The refresh ignored workflow data function:', () => {
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      dummyData = { WorkflowId: 'dummyData' }
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception`)
      await request.batch(`truncate table ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow`)
      await request.batch(`insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow (WORKFLOW_ID) values ('dummyData')`)
    })

    afterAll(async () => {
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception`)
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

      const expectedIgnoredWorkflowData = [dummyData]
      await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData)
    })

    it('should ignore a CSV file with a valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-header-row-no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [dummyData]
      await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData)
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

      await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData)
    })

    it('should ignore a csv that has no header row, only data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-data-rows-no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [dummyData]

      await refreshIgnoredWorkflowDataAndCheckRejectionResults(mockResponseData, expectedIgnoredWorkflowData)
    })

    it('should ignore a csv that has a misspelled header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'headers-misspelled.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedIgnoredWorkflowData = [dummyData]

      await refreshIgnoredWorkflowDataAndCheckRejectionResults(mockResponseData, expectedIgnoredWorkflowData)
    })

    it('should not refresh when a non-csv file is supplied', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'json-file.json',
        statusText: STATUS_TEXT_OK,
        contentType: JSONFILE
      }

      const expectedIgnoredWorkflowData = [dummyData]

      await refreshIgnoredWorkflowDataAndCheckRejectionResults(mockResponseData, expectedIgnoredWorkflowData)
    })

    it('should refresh given a valid CSV file', async () => {
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

      await refreshIgnoredWorkflowDataAndCheckExpectedResults(mockResponseData, expectedIgnoredWorkflowData)
    })

    it('should not refresh if csv endpoint is not found(404)', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_404,
        statusText: STATUS_TEXT_NOT_FOUND,
        contentType: HTML,
        filename: '404-html.html'
      }

      const expectedIgnoredWorkflowData = [dummyData]

      await refreshIgnoredWorkflowDataAndCheckRejectionResults(mockResponseData, expectedIgnoredWorkflowData)
    })

    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error(`connect ECONNREFUSED mockhost`)
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
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)

    it('should load unloadable rows into csv exceptions table', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'invalid-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedErrorDescription = 'A row is missing data.'

      await refreshIgnoredWorkflowDataAndCheckExceptionIsCreated(mockResponseData, expectedErrorDescription)
    })
  })

  async function refreshIgnoredWorkflowDataAndCheckExpectedResults (mockResponseData, expectedIgnoredWorkflowData) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message) // calling actual function here
    await checkExpectedResults(expectedIgnoredWorkflowData)
  }

  // The following function is used in scenarios where a csv is successfully processed, but due to errors in the csv the app will then
  // attempt to overwrite and insert nothing into the database. This is caught and rejected in the function code (hence expecting this error/rejection).
  async function refreshIgnoredWorkflowDataAndCheckRejectionResults (mockResponseData, expectedIgnoredWorkflowData) {
    const expectedError = new Error(`A null database overwrite is not allowed`)
    await mockFetchResponse(mockResponseData)
    await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
    await checkExpectedResults(expectedIgnoredWorkflowData)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/ignored_workflow_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedIgnoredWorkflowData) {
    const result = await request.query(`
        select 
          count(*)
        as 
          number
        from 
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.IGNORED_WORKFLOW
         `)
    const expectedNumberOfRows = expectedIgnoredWorkflowData.length

    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`Live data row count: ${result.recordset[0].number}, test data row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      for (const row of expectedIgnoredWorkflowData) {
        const WorkflowId = row.WorkflowId

        const databaseResult = await request.query(`
        select 
         count(*) 
        as 
          number 
        from 
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.IGNORED_WORKFLOW
        where 
          WORKFLOW_ID = '${WorkflowId}'
        `)
        expect(databaseResult.recordset[0].number).toEqual(1)
      }
    }
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
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} (WORKFLOW_ID) 
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
      ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception
    order by
      exception_time desc
  `)
    expect(result.recordset[0].description).toBe(expectedErrorDescription)
  }
})
