module.exports =
  describe('Insert fluvial_display_group_workflow data tests', () => {
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

    describe('The refresh fluvial_display_group_workflow data function:', () => {
      beforeAll(async () => {
        await pool.connect()
      })

      beforeEach(async () => {
        // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
        // function implementation for the function context needs creating for each test.
        dummyData = {
          dummyWorkflow: {
            dummyPlot: ['dummyLocation']
          }
        }
        context = new Context()
        await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception`)
        await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
        await request.batch(`insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow (workflow_id, plot_id, location_ids) values ('dummyWorkflow', 'dummyPlot', 'dummyLocation')`)
      })

      afterEach(() => {
        // As the jestConnection pool is only closed at the end of the test suite the global temporary table used by each function
        // invocation needs to be dropped manually between each test case.
        return request.batch(`drop table if exists #fluvial_display_group_workflow_temp`)
      })

      afterAll(async () => {
        await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
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

        const expectedDisplayGroupData = dummyData

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should ignore a CSV file with misspelled headers', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'headers-misspelled.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedDisplayGroupData = dummyData

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should load only PlotId, FFFSLocID and WorkflowId into the db correctly, ignoring extra CSV fields', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'extra-headers.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedDisplayGroupData = {
          workflow1: {
            plot1: ['location1', 'location2']
          }
        }

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should group locations by plot ID and workflow ID given single location per workflowId/plotId', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'single-location-per-plot-for-workflow.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedDisplayGroupData = {
          workflow1: {
            plot1: ['location4'],
            plot2: ['location1']
          },
          workflow2: {
            plot1: ['location1']
          }
        }

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should group locations by plot ID and workflow ID given multiple combinations of workflowId and plotId', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'multiple-locations-per-plot-for-workflow.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedDisplayGroupData = {
          workflow1: {
            plot1: ['location1', 'location2', 'location3', 'location4'],
            plot2: ['location1']
          },
          workflow2: {
            plot1: ['location1', 'location2']
          }
        }

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should not refresh with valid header row but no data rows', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'valid-header-row-no-data-rows.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedDisplayGroupData = dummyData

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should reject insert if there is no header row, expect the first row to be treated as the header', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'valid-data-rows-no-header-row.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedDisplayGroupData = dummyData

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should ommit rows with missing values in columns', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'missing-data-in-columns.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedDisplayGroupData = {
          workflow2: {
            plot1: ['location1']
          }
        }

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should ommit all rows as there is missing values for the entire column', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'missing-data-in-entire-column.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedDisplayGroupData = dummyData

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should not refresh when a non-csv file (JSON) is provided', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'json-file.json',
          statusText: STATUS_TEXT_OK,
          contentType: JSONFILE
        }

        const expectedDisplayGroupData = dummyData

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should not refresh if csv endpoint is not found(404)', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_404,
          statusText: STATUS_TEXT_NOT_FOUND,
          contentType: HTML,
          filename: '404-html.html'
        }

        const expectedDisplayGroupData = dummyData

        await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedDisplayGroupData)
      })

      it('should throw an exception when the csv server is unavailable', async () => {
        const expectedError = new Error(`connect ECONNREFUSED mockhost`)
        fetch.mockImplementation(() => {
          throw new Error('connect ECONNREFUSED mockhost')
        })
        await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      })

      it('should throw an exception when the fluvial_display_group_workflow table is being used', async () => {
        // If the fluvial_display_group_workflow table is being refreshed messages are elgible for replay a certain number of times
        // so check that an exception is thrown to facilitate this process.

        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'multiple-locations-per-plot-for-workflow.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        await lockDisplayGroupTableAndCheckMessageCannotBeProcessed(mockResponseData)
        // Set the test timeout higher than the database request timeout.
      }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)

      it('should load unloadable rows into csv exceptions table', async () => {
        const mockResponseData = {
          statusCode: STATUS_CODE_200,
          filename: 'invalid-row.csv',
          statusText: STATUS_TEXT_OK,
          contentType: TEXT_CSV
        }

        const expectedErrorDescription = 'String or binary data would be truncated.'

        await refreshDisplayGroupDataAndCheckExceptionIsCreated(mockResponseData, expectedErrorDescription)
      })
    })

    async function refreshDisplayGroupDataAndCheckExpectedResults (mockResponseData, expectedDisplayGroupData) {
      await mockFetchResponse(mockResponseData)
      await messageFunction(context, message) // This is a call to the function index
      await checkExpectedResults(expectedDisplayGroupData)
    }

    async function mockFetchResponse (mockResponseData) {
      let mockResponse = {}
      mockResponse = {
        status: mockResponseData.statusCode,
        body: fs.createReadStream(`testing/fluvial_display_group_workflow_files/${mockResponseData.filename}`),
        statusText: mockResponseData.statusText,
        headers: { 'Content-Type': mockResponseData.contentType },
        sendAsJson: false
      }
      fetch.mockResolvedValue(mockResponse)
    }

    async function checkExpectedResults (expectedDisplayGroupData) {
      const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
      const workflowIds = Object.keys(expectedDisplayGroupData)
      let expectedNumberOfRows = 0

      // The number of rows returned from the database should be equal to the sum of plot ID elements nested within
      // all workflow ID elements of the expected fluvial_display_group_workflow data.
      for (const workflowId of workflowIds) {
        expectedNumberOfRows += Object.keys(expectedDisplayGroupData[workflowId]).length
      }

      // Query the database and check that the locations associated with each grouping of workflow ID and plot ID are as expected.
      expect(result.recordset[0].number).toBe(expectedNumberOfRows)
      context.log(`databse row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

      if (expectedNumberOfRows > 0) {
        const workflowIds = Object.keys(expectedDisplayGroupData)
        for (const workflowId of workflowIds) { // ident single workflowId within expected data
          const plotIds = expectedDisplayGroupData[`${workflowId}`] // ident group of plot ids for workflowId
          for (const plotId in plotIds) { // ident single plot id within workflowId to access locations
            // expected data layout
            const locationIds = plotIds[`${plotId}`] // ident group of location ids for single plotid and single workflowid combination
            const expectedLocationsArray = locationIds.sort()

            // actual db data
            const locationQuery = await request.query(`
          SELECT *
          FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow
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

    async function lockDisplayGroupTableAndCheckMessageCannotBeProcessed (mockResponseData) {
      let transaction
      const tableName = 'fluvial_display_group_workflow'
      try {
        transaction = new sql.Transaction(pool)
        await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
        const request = new sql.Request(transaction)
        await request.batch(`
        insert into 
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} (workflow_id, plot_id, location_ids)
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

    async function refreshDisplayGroupDataAndCheckExceptionIsCreated (mockResponseData, expectedErrorDescription) {
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
  }
  )
