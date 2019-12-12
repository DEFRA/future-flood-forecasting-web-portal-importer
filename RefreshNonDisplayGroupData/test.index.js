// module.exports =
//   describe('Insert fluvial_non_display_group_workflow data tests', () => {
//     const message = require('../testing/mocks/defaultMessage')
//     const Context = require('../testing/mocks/defaultContext')
//     const Connection = require('../Shared/connection-pool')
//     const messageFunction = require('./index')
//     const fetch = require('node-fetch')
//     const sql = require('mssql')
//     const fs = require('fs')

//     // const JSONFILE = 'application/javascript'
//     // const STATUS_TEXT_NOT_FOUND = 'Not found'
//     const STATUS_CODE_200 = 200
//     // const STATUS_CODE_404 = 404
//     const STATUS_TEXT_OK = 'OK'
//     const TEXT_CSV = 'text/csv'
//     // const HTML = 'html'

//     jest.mock('node-fetch')

//     let context

//     const jestConnection = new Connection()
//     const pool = jestConnection.pool
//     const request = new sql.Request(pool)

//     describe('The refresh fluvial_non_display_group_workflow data function', () => {
//       beforeAll(() => {
//         return pool.connect()
//       })

//       beforeEach(() => {
//         // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
//         // function implementation for the function context needs creating for each test.
//         context = new Context()
//         return request.batch(`truncate table ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_non_display_group_workflow`)
//       })

//       afterEach(() => {
//         // As the jestConnection pool is only closed at the end of the test suite the global temporary table used by each function
//         // invocation needs to be dropped manually between each test case.
//         return request.batch(`drop table if exists #fluvial_display_group_workflow_temp`)
//       })

//       afterAll(() => {
//         // Closing the DB connection allows Jest to exit successfully.
//         return pool.close()
//       })
//       // it('should ignore an empty CSV file', async () => {
//       //   const mockResponseData = {
//       //     statusCode: STATUS_CODE_200,
//       //     filename: 'empty.csv',
//       //     statusText: STATUS_TEXT_OK,
//       //     contentType: TEXT_CSV
//       //   }

//       //   const expectedNonDisplayGroupData = {}

//       //   await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
//       // })
//       it('should load a valid csv correctly', async () => {
//         const mockResponseData = {
//           statusCode: STATUS_CODE_200,
//           filename: 'multiple-filters-per-workflow.csv',
//           statusText: STATUS_TEXT_OK,
//           contentType: TEXT_CSV
//         }

//         const expectedNonDisplayGroupData = {
//           'test_non_display_workflow_1': ['test_filter_1', 'test_filter_1a'],
//           'test_non_display_workflow_3': ['test_filter_3'],
//           'test_non_display_workflow_2': ['test_filter_2'],
//           'test_non_display_workflow_4': ['test_filter_4']
//         }

//         await refreshNonDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
//       })
//     })

//     async function refreshNonDisplayGroupDataAndCheckExpectedResults (mockResponseData, expectedNonDisplayGroupData) {
//       await mockFetchResponse(mockResponseData)
//       await messageFunction(context, message) // This is a call to the function index
//       await checkExpectedResults(expectedNonDisplayGroupData)
//     }

//     async function mockFetchResponse (mockResponseData) {
//       let mockResponse = {}
//       mockResponse = {
//         status: mockResponseData.statusCode,
//         body: fs.createReadStream(`testing/fluvial_non_display_group_workflow_files/${mockResponseData.filename}`),
//         statusText: mockResponseData.statusText,
//         headers: { 'Content-Type': mockResponseData.contentType },
//         sendAsJson: false
//       }
//       fetch.mockResolvedValue(mockResponse)
//     }

//     async function checkExpectedResults (expectedNonDisplayGroupData) {
//       const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_non_display_group_workflow`)
//       const workflowIds = Object.keys(expectedNonDisplayGroupData)
//       let expectedNumberOfRows = 0

//       // The number of rows returned from the database should be equal to the sum of the elements nested within the expected fluvial_non_display_group_workflow expected data.
//       for (const workflowId of workflowIds) {
//         expectedNumberOfRows += Object.keys(expectedNonDisplayGroupData[workflowId]).length
//       }

//       // Query the database and check that the filter IDs associated with each workflow ID are as expected.
//       expect(result.recordset[0].number).toBe(expectedNumberOfRows)
//       context.log(`databse row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

//       if (expectedNumberOfRows > 0) {
//         const workflowIds = Object.keys(expectedNonDisplayGroupData)
//         for (const workflowId of workflowIds) { // ident single workflowId within expected data
//           const expectedFilterIds = expectedNonDisplayGroupData[`${workflowId}`] // ident group of filter ids for workflowId

//           // actual db data
//           const filterQuery = await request.query(`
//           SELECT *
//           FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_Non_display_group_workflow
//           WHERE workflow_id = '${workflowId}'
//           `)
//           let rows = filterQuery.recordset
//           let dbFilterIds = []
//           rows.forEach(row =>
//             // console.log('gere')
//             dbFilterIds.push(row.FILTER_ID)
//           )
//           // get an array of filter ids for a given workflow id from the database
//           expect(dbFilterIds).toEqual(expectedFilterIds)
//         }
//       }
//     }
//     async function lockNonDisplayGroupTableAndCheckMessageCannotBeProcessed (mockResponseData) {
//       let transaction
//       try {
//         // Lock the fluvial_display_group_workflow table and then try and process the message.
//         transaction = new sql.Transaction(pool)
//         await transaction.begin()
//         const request = new sql.Request(transaction)
//         await request.batch(`
//         select
//           *
//         from
//           ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_non_display_group_workflow
//         with
//           (tablock, holdlock)
//       `)
//         await mockFetchResponse(mockResponseData)
//         await messageFunction(context, message)
//       } catch (err) {
//         // Check that a request timeout occurs.
//         expect(err.code).toTimeout(err.code)
//       } finally {
//         try {
//           await transaction.rollback()
//         } catch (err) { }
//       }
//     }
//   })
