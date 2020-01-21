module.exports =
  describe('Insert non_display_group_workflow data tests', () => {
    const message = require('../testing/mocks/defaultMessage')
    const Context = require('../testing/mocks/defaultContext')
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

    describe('The refresh non_display_group_workflow data function', () => {
      beforeAll(() => {
        return pool.connect()
      })

      beforeEach(() => {
        // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
        // function implementation for the function context needs creating for each test.
        context = new Context()
        return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow`)
      })

      beforeEach(() => {
        dummyData = {
          dummyWorkflow: ['dummyFilter']
        }
        return request.batch(`INSERT INTO ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow (workflow_id, filter_id) values ('dummyWorkflow', 'dummyFilter')`)
      })

      afterAll(() => {
        // Closing the DB connection allows Jest to exit successfully.
        return pool.close()
      })
      it('should ignore an empty CSV file', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'empty.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = dummyData

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should load a valid csv correctly - single filter per workflow', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'single-filter-per-workflow.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = {
          test_non_display_workflow_1: ['test_filter_1'],
          test_non_display_workflow_3: ['test_filter_3'],
          test_non_display_workflow_2: ['test_filter_2']
        }

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should load a valid csv correctly - multiple filters per workflow', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'multiple-filters-per-workflow.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = {
          test_non_display_workflow_1: ['test_filter_1', 'test_filter_1a'],
          test_non_display_workflow_3: ['test_filter_3'],
          test_non_display_workflow_2: ['test_filter_2'],
          test_non_display_workflow_4: ['test_filter_4']
        }

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should not load duplicate rows in a csv', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'duplicate-rows.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = {
          test_non_display_workflow_1: ['test_filter_1'],
          test_non_display_workflow_3: ['test_filter_3'],
          test_non_display_workflow_2: ['test_filter_2']
        }

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should ignore a CSV file with misspelled headers', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'headers-misspelled.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = dummyData

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should load WorkflowId and FilterId correctly into the db correctly with extra CSV fields present', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'extra-headers.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = {
          test_non_display_workflow_1: ['test_filter_1'],
          test_non_display_workflow_2: ['test_filter_2']
        }

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should not refresh with valid header row but no data rows', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'valid-header-row-no-data-rows.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = dummyData

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should reject insert if there is no header row, expect the first row to be treated as the header', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'valid-data-rows-no-header-row.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = dummyData

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should ommit rows with missing values', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'missing-data-in-some-rows.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = {
          test_non_display_workflow_2: ['test_filter_a']
        }

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should ommit all rows as there is missing values for the entire column', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'missing-data-in-entire-column.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedNonDisplayGroupData = dummyData

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should not refresh when a non-csv file (JSON) is provided', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'json-file.json',
          statusText: STATUS_TEXT_OK,
          contentType: JSONFILE
        }

        const expectedNonDisplayGroupData = dummyData

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should not refresh if csv endpoint is not found(404)', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_404,
          statusText: STATUS_TEXT_NOT_FOUND,
          contentType: HTML,
          filename: '404-html.html'
        }

        const expectedNonDisplayGroupData = dummyData

        await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
      })
      it('should throw an exception when the csv server is unavailable', async () => {
        const expectedError = new Error(`connect ECONNREFUSED mockhost`)
        fetch.mockImplementation(() => {
          throw new Error('connect ECONNREFUSED mockhost')
        })
        await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      })
      it('should throw an exception when the non_display_group_workflow table is being used', async () => {
        // If the non_display_group_workflow table is being refreshed messages are elgible for replay a certain number of times
        // so check that an exception is thrown to facilitate this process.

        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'single-filter-per-workflow.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        await lockNonDisplayGroupTableAndCheckMessageCannotBeProcessed(mockResponseData)
        // Set the test timeout higher than the database request timeout.
      }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    })

    async function refreshNonDisplayGroupDataAndCheckExpectedResults (mockResponseData, expectedNonDisplayGroupData) {
      await mockFetchResponse(mockResponseData)
      await messageFunction(context, message) // This is a call to the function index
      await checkExpectedResults(expectedNonDisplayGroupData)
    }

    async function mockFetchResponse (mockResponseData) {
      let mockResponse = {}
      mockResponse = {
        status: mockResponseData.statusCode,
        body: fs.createReadStream(`testing/non_display_group_workflow_files/${mockResponseData.filename}`),
        statusText: mockResponseData.statusText,
        headers: { 'Content-Type': mockResponseData.contentType },
        sendAsJson: false
      }
      fetch.mockResolvedValue(mockResponse)
    }

    async function checkExpectedResults (expectedNonDisplayGroupData) {
      const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow`)
      const workflowIds = Object.keys(expectedNonDisplayGroupData)
      let expectedNumberOfRows = 0

      // The number of rows returned from the database should be equal to the sum of the elements nested within the expected non_display_group_workflow expected data.
      for (const workflowId of workflowIds) {
        expectedNumberOfRows += Object.keys(expectedNonDisplayGroupData[workflowId]).length
      }

      // Query the database and check that the filter IDs associated with each workflow ID are as expected.
      expect(result.recordset[0].number).toBe(expectedNumberOfRows)
      context.log(`databse row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

      if (expectedNumberOfRows > 0) {
        const workflowIds = Object.keys(expectedNonDisplayGroupData)
        for (const workflowId of workflowIds) { // ident single workflowId within expected data
          const expectedFilterIds = expectedNonDisplayGroupData[`${workflowId}`] // ident group of filter ids for workflowId

          // actual db data
          const filterQuery = await request.query(`
          SELECT *
          FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.non_display_group_workflow
          WHERE workflow_id = '${workflowId}'
          `)
          const rows = filterQuery.recordset
          const dbFilterIds = []
          rows.forEach(row =>
            dbFilterIds.push(row.FILTER_ID)
          )
          const dbFilterIdsSorted = dbFilterIds.sort()
          const expectedFilterIdsSorted = expectedFilterIds.sort()
          // get an array of filter ids for a given workflow id from the database
          expect(dbFilterIdsSorted).toEqual(expectedFilterIdsSorted)
        }
      }
    }
    async function lockNonDisplayGroupTableAndCheckMessageCannotBeProcessed (mockResponseData) {
      let transaction
      const tableName = 'non_display_group_workflow'
      try {
        transaction = new sql.Transaction(pool)
        await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
        const request = new sql.Request(transaction)
        await request.batch(`
          INSERT INTO 
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} (workflow_id, filter_id) 
          values 
          ('testWorkflow', 'testFilter')`)
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
  }
  )
