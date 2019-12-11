// module.exports =
//   describe('Insert fluvial_non_display_group_workflow data tests', () => {
//     const message = require('../testing/mocks/defaultMessage')
//     const Context = require('../testing/mocks/defaultContext')
//     const Connection = require('../Shared/connection-pool')
//     const messageFunction = require('./index')
//     const fetch = require('node-fetch')
//     const sql = require('mssql')
//     const fs = require('fs')

//     const JSONFILE = 'application/javascript'
//     const STATUS_TEXT_NOT_FOUND = 'Not found'
//     const STATUS_CODE_200 = 200
//     const STATUS_CODE_404 = 404
//     const STATUS_TEXT_OK = 'OK'
//     const TEXT_CSV = 'text/csv'
//     const HTML = 'html'

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
//       it('should ignore an empty CSV file', async () => {
//         const mockResponseData = {
//           statusCode: STATUS_CODE_200,
//           filename: 'empty.csv',
//           statusText: STATUS_TEXT_OK,
//           contentType: TEXT_CSV
//         }

//         const expectedNonDisplayGroupData = {}

//         await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
//       })
//       it('should load a valid csv correctly', async () => {
//         const mockResponseData = {
//           statusCode: STATUS_CODE_200,
//           filename: 'extra-headers.csv',
//           statusText: STATUS_TEXT_OK,
//           contentType: TEXT_CSV
//         }

//         const expectedNonDisplayGroupData = {
//           'dummyNonDisplayworkflow1': ['dummyFilter1', 'dummyFilter2'],
//           'dummyNonDisplayworkflow2': 'dummyFilter3'
//         }

//         await refreshDisplayGroupDataAndCheckExpectedResults(mockResponseData, expectedNonDisplayGroupData)
//       })
//     })

//     async function refreshDisplayGroupDataAndCheckExpectedResults (mockResponseData, expectedNonDisplayGroupData) {
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

//       // The number of rows returned from the database should be equal to the sum of the elements nested within
//       // the expected fluvial_display_group_workflow data.
//       for (const workflowId of workflowIds) {
//         expectedNumberOfRows += Object.keys(expectedNonDisplayGroupData[workflowId]).length
//       }

//       // Query the database and check that the locations associated with each grouping of workflow ID and plot ID areas expected.
//       expect(result.recordset[0].number).toBe(expectedNumberOfRows)
//       context.log(`databse row count: ${result.recordset[0].number}, input csv row count: ${expectedNumberOfRows}`)

//       if (expectedNumberOfRows > 0) {
//         const workflowIds = Object.keys(expectedDisplayGroupData)
//         for (const workflowId of workflowIds) { // ident single workflowId within expected data
//           const plotIds = expectedDisplayGroupData[`${workflowId}`] // ident group of plot ids for workflowId
//           for (const plotId in plotIds) { // ident single plot id within workflowId to access locations
//             // expected data layout
//             const locationIds = plotIds[`${plotId}`] // ident group of location ids for single plotid and single workflowid combination
//             const expectedLocationsArray = locationIds.sort()

//             // actual db data
//             const locationQuery = await request.query(`
//           SELECT *
//           FROM ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow
//           WHERE workflow_id = '${workflowId}' AND plot_id = '${plotId}'
//           `)
//             const rows = locationQuery.recordset
//             const dbLocationsResult = rows[0].LOCATION_IDS
//             const dbLocations = dbLocationsResult.split(';').sort()
//             expect(dbLocations).toEqual(expectedLocationsArray)
//           }
//         }
//       }
//     }
//   })
