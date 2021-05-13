import messageFunction from '../../../RefreshLocationThresholdsData/index.mjs'
import CommonWorkflowCSVTestUtils from '../shared/common-workflow-csv-test-utils.js'
import Util from '../shared/common-csv-refresh-utils.js'
import ConnectionPool from '../../../Shared/connection-pool.js'
import Context from '../mocks/defaultContext.js'
import message from '../mocks/defaultMessage.js'
import fetch from 'node-fetch'
import sql from 'mssql'
import fs from 'fs'
import { jest } from '@jest/globals'

jest.mock('node-fetch')

export const refreshLocationThresholdsDataTests = () => describe('Refresh location thresholds data tests', () => {
  const STATUS_CODE_200 = 200
  const STATUS_TEXT_OK = 'OK'
  const TEXT_CSV = 'text/csv'
  const HTML = 'html'

  let context
  let dummyData
  let commonCSVTestUtils
  let commonWorkflowCSVTestUtils

  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const request = new sql.Request(pool)

  describe('The refresh location thresholds data function:', () => {
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.serviceConfigurationUpdateCompleted = []
      commonCSVTestUtils = new Util(context, pool)
      commonWorkflowCSVTestUtils = new CommonWorkflowCSVTestUtils(context, pool)
      dummyData = [{ LOCATIONID: 'dummyData', ID: 'dummyData', NAME: 'dummyData', LABEL: 'dummyData', VALUE: 1234, FLUVIALTYPE: 'dummyData', COMMENT: 'dummyData', DESCRIPTION: 'dummyData' }]
      await request.batch('delete from fff_staging.csv_staging_exception')
      await request.batch('delete from fff_staging.ungrouped_location_thresholds')
      await request.batch(`
      insert 
        into fff_staging.ungrouped_location_thresholds
        (LOCATION_ID, THRESHOLD_ID, NAME, LABEL, VALUE, FLUVIAL_TYPE, COMMENT, DESCRIPTION) 
      values 
        ('dummyData', 'dummyData', 'dummyData', 'dummyData', 1234, 'dummyData', 'dummyData', 'dummyData')
      `)
      await request.query('delete from fff_staging.non_workflow_refresh')
      await request.query('delete from fff_staging.workflow_refresh')
    })

    afterAll(async () => {
      await request.batch('delete from fff_staging.ungrouped_location_thresholds')
      await request.batch('delete from fff_staging.csv_staging_exception')
      await request.query('delete from fff_staging.non_workflow_refresh')
      await request.query('delete from fff_staging.workflow_refresh')
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

      const expectedLocationThresholdsData = dummyData
      const expectedNumberOfExceptionRows = 0
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a CSV file with a valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = dummyData
      const expectedNumberOfExceptionRows = 0
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should only load data rows that are complete within a csv that has some incomplete rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'some-data-rows-missing-values.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = [
        {
          LOCATIONID: '0130TH',
          ID: 'RES FAL All',
          NAME: 'RES FAL',
          LABEL: 'RES FAL 061WAF23EwnCrkld',
          VALUE: 0.99,
          FLUVIALTYPE: 'Level',
          COMMENT: 'Some Comment',
          DESCRIPTION: 'RES FAL 061WAF23EwnCrkld'
        }]
      const expectedNumberOfExceptionRows = 1
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has all rows with missing values', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'all-data-rows-missing-some-values.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should ignore rows that contains values exceeding a specified limit', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'one-row-has-data-over-specified-limits.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = [
        {
          LOCATIONID: '0130TH',
          ID: 'RES FAL All',
          NAME: 'RES FAL',
          LABEL: 'RES FAL 061WAF23EwnCrkld',
          VALUE: 0.99,
          FLUVIALTYPE: 'Level',
          COMMENT: 'Some Comment',
          DESCRIPTION: 'RES FAL 061WAF23EwnCrkld'
        }]
      const expectedNumberOfExceptionRows = 0
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has a string value in a decimal field', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'string-not-decimal.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has no header row, only data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = dummyData
      const expectedNumberOfExceptionRows = 1
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has a missing header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has a misspelled header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'misspelled-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should refresh given a valid CSV file', async () => {
      // Disable core engine message processing explicitly.
      process.env['AzureWebJobs.ProcessFewsEventCode.Disabled'] = 'true'
      process.env['AzureWebJobs.ImportFromFews.Disabled'] = 'true'

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = [{
        LOCATIONID: '0130TH',
        ID: 'ACT EDF All',
        NAME: 'ACT EDF',
        LABEL: 'ACT EDF',
        VALUE: 0.79,
        FLUVIALTYPE: 'Level',
        COMMENT: 'Alarm Level',
        DESCRIPTION: 'ACT EDF'
      },
      {
        LOCATIONID: '0130TH',
        ID: 'RES FAL All',
        NAME: 'RES FAL',
        LABEL: 'RES FAL 061WAF23EwnCrkld',
        VALUE: 0.99,
        FLUVIALTYPE: 'Level',
        COMMENT: 'Some Comment',
        DESCRIPTION: 'RES FAL 061WAF23EwnCrkld'
      }]
      const expectedNumberOfExceptionRows = 0
      const expectedServiceConfigurationUpdateNotification = false
      // Ensure a service configuration update is not detected.
      await commonCSVTestUtils.insertNonWorkflowRefreshRecords(-500)
      await commonWorkflowCSVTestUtils.insertWorkflowRefreshRecords()
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification)
    })

    it('should not refresh given a valid CSV file with null values in some of all row cells', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'empty-values-in-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })

    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error('connect ECONNREFUSED mockhost')
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
    })

    it('should throw an exception when the location thresholds table is in use', async () => {
      // If the location thresholds table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      await lockLocationThresholdsTableAndCheckMessageCannotBeProcessed(mockResponseData)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 5000)

    it('should not refresh when a non-csv file (JSON) is provided', async () => {
      const mockResponse = {
        status: STATUS_CODE_200,
        body: fs.createReadStream('testing/function-tests/general-files/json.json'),
        statusText: STATUS_TEXT_OK,
        headers: { 'Content-Type': 'application/javascript' },
        url: '.json'
      }
      await fetch.mockResolvedValue(mockResponse)

      const expectedData = dummyData
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error('No csv file detected')

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData, expectedNumberOfExceptionRows)
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

      const expectedData = dummyData
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error('No csv file detected')

      await expect(messageFunction(context, message)).rejects.toEqual(expectedError)
      await checkExpectedResults(expectedData, expectedNumberOfExceptionRows)
    })

    it('should refresh given a valid CSV file with a null comment value', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid-null-comment.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedLocationThresholdsData = [{
        LOCATIONID: '0130TH',
        ID: 'ACT EDF All',
        NAME: 'ACT EDF',
        LABEL: 'ACT EDF',
        VALUE: 0.79,
        FLUVIALTYPE: 'Level',
        COMMENT: 'Alarm Level',
        DESCRIPTION: 'ACT EDF'
      },
      {
        LOCATIONID: '0130TH',
        ID: 'RES FAL All',
        NAME: 'RES FAL',
        LABEL: 'RES FAL 061WAF23EwnCrkld',
        VALUE: 0.99,
        FLUVIALTYPE: 'Level',
        DESCRIPTION: 'RES FAL 061WAF23EwnCrkld'
      }]
      const expectedNumberOfExceptionRows = 0
      await refreshLocationThresholdsDataAndCheckExpectedResults(mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows)
    })
  })

  async function refreshLocationThresholdsDataAndCheckExpectedResults (mockResponseData, expectedLocationThresholdsData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message) // calling actual function here
    await checkExpectedResults(expectedLocationThresholdsData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/function-tests/RefreshLocationThresholdsData/location_thresholds_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false,
      url: '.csv'
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedLocationThresholdsData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification) {
    const result = await request.query(`
      select 
        count(*) 
      as
        number
      from 
        fff_staging.ungrouped_location_thresholds
    `)
    const expectedNumberOfRows = expectedLocationThresholdsData.length

    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`Live data row count: ${result.recordset[0].number}, test data row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      for (const row of expectedLocationThresholdsData) {
        const LOCATIONID = row.LOCATIONID
        const ID = row.ID
        const NAME = row.NAME
        const LABEL = row.LABEL
        const VALUE = row.VALUE
        const FLUVIALTYPE = row.FLUVIALTYPE
        const COMMENT = row.COMMENT ? `= '${row.COMMENT}'` : 'is null'
        const DESCRIPTION = row.DESCRIPTION

        const databaseResult = await request.query(`
        select 
          count(*) 
        as 
          number 
        from 
          fff_staging.ungrouped_location_thresholds
        where 
          LOCATION_ID = '${LOCATIONID}' and THRESHOLD_ID = '${ID}'
          and NAME = '${NAME}' and LABEL = '${LABEL}' and VALUE = ${VALUE}
          and FLUVIAL_TYPE = '${FLUVIALTYPE}' and COMMENT ${COMMENT}
          and DESCRIPTION = '${DESCRIPTION}'
        `)
        expect(databaseResult.recordset[0].number).toEqual(1)
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
        fff_staging.csv_staging_exception`)
      expect(exceptionCount.recordset[0].number).toBe(expectedNumberOfExceptionRows)
    }

    // Check the expected service configuration update notification status.
    await commonCSVTestUtils.checkExpectedServiceConfigurationUpdateNotificationStatus(context, expectedServiceConfigurationUpdateNotification)
  }

  async function lockLocationThresholdsTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    const tableName = 'location_thresholds'
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(`
        insert into 
          fff_staging.${tableName} (LOCATION_ID, THRESHOLD_ID, NAME, LABEL, VALUE, FLUVIAL_TYPE, COMMENT, DESCRIPTION) 
        values 
          ('0130TH', 'ACT EDF All', 'ACT EDF', 'ACT EDF', 0.79, 'Level', 'Alarm Level', 'ACT EDF')
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
})
