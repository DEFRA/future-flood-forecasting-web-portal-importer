module.exports =
  describe('Insert coastal_display_group_workflow data tests', () => {
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

    describe('The refresh coastal_display_group_workflow data function:', () => {
      beforeAll(async () => {
        await pool.connect()
      })

      beforeEach(async () => {
        // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
        // function implementation for the function context needs creating for each test.
        context = new Context()
        dummyData = {
          dummyWorkflow: {
            dummyPlot: ['dummyLocation']
          }
        }
        await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception`)
        await request.query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_display_group_workflow`)
        await request.query(`insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_display_group_workflow (workflow_id, plot_id, fffs_loc_ids) values ('dummyWorkflow', 'dummyPlot', 'dummyLocation')`)
      })

      afterEach(() => {
        // As the jestConnection pool is only closed at the end of the test suite the global temporary table used by each function
        // invocation needs to be dropped manually between each test case.
        return request.query(`drop table if exists #coastal_display_group_workflow_temp`)
      })

      afterAll(async () => {
        await request.query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_display_group_workflow`)
        await request.query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception`)
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

        const expectedCoastalDisplayGroupData = dummyData

        const expectedNumberOfExceptionRows = 0
        await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
      })
      it('should refresh given a valid CSV file (even with extra csv fields)', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'valid.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedCoastalDisplayGroupData = {
          BE: {
            StringTRITON_outputs_BER: ['St1', 'St2'],
            TRITON_outputs_Other: ['St3']
          },
          Workflow2: {
            Plot3: ['St4']
          }
        }

        const expectedNumberOfExceptionRows = 0
        await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
      })
      it('should ignore a CSV file with misspelled headers', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'headers-misspelled.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedCoastalDisplayGroupData = dummyData

        const expectedNumberOfExceptionRows = 2
        const expectedErrorDescription = 'row is missing data'
        await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
        await checkExceptionIsCorrect(expectedErrorDescription)
      })
      it('should not refresh with valid header row but no data rows', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'valid-header-row-no-data-rows.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedCoastalDisplayGroupData = dummyData

        const expectedNumberOfExceptionRows = 0
        await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
      })
      it('should reject insert if there is no header row, expect the first row to be treated as the header', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'valid-data-rows-no-header-row.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedCoastalDisplayGroupData = dummyData

        const expectedNumberOfExceptionRows = 3
        const expectedErrorDescription = 'row is missing data'
        await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
        await checkExceptionIsCorrect(expectedErrorDescription)
      })
      it('should load rows with missing values in columns into exceptions', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'missing-data-in-a-column.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedCoastalDisplayGroupData = {
          BE: {
            StringTRITON_outputs_BER: ['St2'],
            TRITON_outputs_Other: ['St3']
          },
          Workflow2: {
            Plot3: ['St4']
          }
        }

        const expectedNumberOfExceptionRows = 1
        const expectedErrorDescription = 'row is missing data'
        await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
        await checkExceptionIsCorrect(expectedErrorDescription)
      })
      it('should ommit all rows as there is missing values for the entire column', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'missing-data-in-entire-column.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedCoastalDisplayGroupData = dummyData

        const expectedNumberOfExceptionRows = 4
        const expectedErrorDescription = 'row is missing data'
        await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
        await checkExceptionIsCorrect(expectedErrorDescription)
      })
      it('should load a row with fields exceeding data limits into exceptions', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'exceeding-data-limit.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedCoastalDisplayGroupData = dummyData

        const expectedNumberOfExceptionRows = 1
        const expectedErrorDescription = 'data would be truncated.'
        await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
        await checkExceptionIsCorrect(expectedErrorDescription)
      })
      it('should load an incomplete row into exceptions', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'incomplete-row.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedCoastalDisplayGroupData = dummyData

        const expectedNumberOfExceptionRows = 1
        const expectedErrorDescription = 'row is missing data'
        await refreshCoastalDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
        await checkExceptionIsCorrect(expectedErrorDescription)
      })
      it('should not refresh when a non-csv file (JSON) is provided', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'json-file.json',
          statusText: STATUS_TEXT_OK,
          contentType: JSONFILE
        }

        const expectedCoastalLocationData = dummyData
        const expectedNumberOfExceptionRows = 0
        const expectedError = new Error(`No csv file detected`)
        await refreshCoastalDisplayGroupDataAndCheckFail(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows, expectedError)
      })
      it('should not refresh if csv endpoint is not found(404)', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_404,
          statusText: STATUS_TEXT_NOT_FOUND,
          contentType: HTML,
          filename: '404-html.html'
        }

        const expectedCoastalLocationData = dummyData
        const expectedNumberOfExceptionRows = 0
        const expectedError = new Error(`No csv file detected`)
        await refreshCoastalDisplayGroupDataAndCheckFail(mockResponseData, expectedCoastalLocationData, expectedNumberOfExceptionRows, expectedError)
      })
      it('should throw an exception when the csv server is unavailable', async () => {
        const expectedError = new Error(`connect ECONNREFUSED mockhost`)
        fetch.mockImplementation(() => {
          throw new Error('connect ECONNREFUSED mockhost')
        })
        await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      })
      it('should throw an exception when the coastal_display_group_workflow table is being used', async () => {
        // If the coastal_display_group_workflow table is being refreshed messages are eligible for replay a certain number of times
        // so check that an exception is thrown to facilitate this process.
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'valid.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        await lockCoastalDisplayGroupTableAndCheckMessageCannotBeProcessed(mockResponseData)
        // Set the test timeout higher than the database request timeout.
      }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    })

    async function refreshCoastalDisplayGroupDataAndCheckExpectedResults (mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows) {
      await mockFetchResponse(mockResponseData)
      await messageFunction(context, message) // This is a call to the function index
      await checkExpectedResults(expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
    }

    async function refreshCoastalDisplayGroupDataAndCheckFail (mockResponseData, expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows, expectedError) {
      await mockFetchResponse(mockResponseData)
      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows)
    }

    async function mockFetchResponse (mockResponseData) {
      let mockResponse = {}
      mockResponse = {
        status: mockResponseData.statusCode,
        body: fs.createReadStream(`testing/coastal_display_group_workflow_files/${mockResponseData.filename}`),
        statusText: mockResponseData.statusText,
        headers: { 'Content-Type': mockResponseData.contentType },
        sendAsJson: false
      }
      fetch.mockResolvedValue(mockResponse)
    }

    async function checkExpectedResults (expectedCoastalDisplayGroupData, expectedNumberOfExceptionRows) {
      const tableCountResult = await request.query(`
      select 
        count(*) 
      as 
        number 
      from 
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_display_group_workflow`)
      // The number of rows (each workflow - plot combination) returned from the database should be equal to the sum of plot ID elements nested within
      // all workflow ID elements of the expected coastal_display_group_workflow data.
      let expectedNumberOfRows = 0
      for (const workflowId in expectedCoastalDisplayGroupData) {
        expectedNumberOfRows += Object.keys(expectedCoastalDisplayGroupData[workflowId]).length
      }

      // Query the database and check that the locations associated with each grouping of workflow ID and plot ID are as expected.
      expect(tableCountResult.recordset[0].number).toBe(expectedNumberOfRows)
      context.log(`databse row count: ${tableCountResult.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

      if (expectedNumberOfRows > 0) {
        for (const workflowId in expectedCoastalDisplayGroupData) { // ident single workflowId within expected data
          const plotIds = expectedCoastalDisplayGroupData[`${workflowId}`] // ident group of plot ids for workflowId
          for (const plotId in plotIds) {
            const locationIds = plotIds[`${plotId}`] // ident group of location ids for single plotid and single workflowid combination
            const expectedLocationsArray = locationIds.sort()

            // actual db data
            const locationQuery = await request.query(`
          select *
          from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.coastal_display_group_workflow
          where workflow_id = '${workflowId}' AND plot_id = '${plotId}'
          `)
            const dbRows = locationQuery.recordset
            const dbLocationsResult = dbRows[0].FFFS_LOC_IDS
            const dbLocations = dbLocationsResult.split(';').sort()
            expect(dbLocations).toEqual(expectedLocationsArray)
          }
        }
      }
      // Check exceptions
      if (expectedNumberOfExceptionRows) {
        const exceptionCount = await request.query(`
        select 
          count(*)
        as 
          number 
        from 
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception`)
        expect(exceptionCount.recordset[0].number).toBe(expectedNumberOfExceptionRows)
      }
    }

    async function lockCoastalDisplayGroupTableAndCheckMessageCannotBeProcessed (mockResponseData) {
      let transaction
      const tableName = 'coastal_display_group_workflow'
      try {
        transaction = new sql.Transaction(pool)
        await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
        const request = new sql.Request(transaction)
        await request.query(`
        insert into 
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} (workflow_id, plot_id, fffs_loc_ids)
        values 
        ('workflow_id', 'plot_id', 'loc_id')
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

    async function checkExceptionIsCorrect (expectedErrorDescription) {
      const result = await request.query(`
      select
        top(1) description
      from
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception
      order by
        exception_time desc
    `)
      expect(result.recordset[0].description).toContain(expectedErrorDescription)
    }
  }
  )
