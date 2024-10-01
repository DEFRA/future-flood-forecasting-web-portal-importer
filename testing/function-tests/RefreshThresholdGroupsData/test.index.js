import messageFunction from '../../../RefreshThresholdGroupsData/index.mjs'
import CommonWorkflowCSVTestUtils from '../shared/common-workflow-csv-test-utils.js'
import Util from '../shared/common-csv-refresh-utils.js'
import ConnectionPool from '../../../Shared/connection-pool.js'
import Context from '../mocks/defaultContext.js'
import message from '../mocks/defaultMessage.js'
import fetch from 'node-fetch'
import sql from 'mssql'
import fs from 'fs'
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'

export const refreshThresholdGroupsDataTests = () => describe('Refresh threshold groups data tests', () => {
  const STATUS_CODE_200 = 200
  const STATUS_TEXT_OK = 'OK'
  const TEXT_CSV = 'text/csv'
  const HTML = 'html'

  let context
  let dummyData
  let commonCSVTestUtils
  let commonWorkflowCSVTestUtils

  const viConnectionPool = new ConnectionPool()
  const pool = viConnectionPool.pool
  const request = new sql.Request(pool)

  describe('The refresh threshold groups data function:', () => {
    beforeAll(async () => {
      await pool.connect()
    })

    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Vitest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      context.bindings.serviceConfigurationUpdateCompleted = []
      commonCSVTestUtils = new Util(context, pool)
      commonWorkflowCSVTestUtils = new CommonWorkflowCSVTestUtils(context, pool)
      dummyData = [{ groupID: 'dummyData', groupName: 'dummyData', thresholdID: 'dummyData', thresholdName: 'dummyData', shortName: 'dummyData' }]
      await request.batch('delete from fff_staging.csv_staging_exception')
      await request.batch('delete from fff_staging.threshold_groups')
      await request.batch(`
      insert 
        into fff_staging.threshold_groups
        (GROUP_ID, GROUP_NAME, THRESHOLD_ID, THRESHOLD_NAME, SHORT_NAME) 
      values 
        ('dummyData', 'dummyData', 'dummyData', 'dummyData', 'dummyData')
      `)
      await request.query('delete from fff_staging.non_workflow_refresh')
      await request.query('delete from fff_staging.workflow_refresh')
    })

    afterAll(async () => {
      await request.batch('delete from fff_staging.threshold_groups')
      await request.batch('delete from fff_staging.csv_staging_exception')
      await request.query('delete from fff_staging.non_workflow_refresh')
      await request.query('delete from fff_staging.workflow_refresh')
      // Closing the DB connection allows Vitest to exit successfully.
      await pool.close()
    })

    it('should ignore an empty CSV file', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'empty.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedThresholdGroupsData = dummyData
      const expectedNumberOfExceptionRows = 0
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a CSV file with a valid header row but no data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedThresholdGroupsData = dummyData
      const expectedNumberOfExceptionRows = 0
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows)
    })

    it('should only load data rows that are complete within a csv that has some incomplete rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'some-data-rows-missing-values.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedThresholdGroupsData = [
        {
          groupID: 'Flood Warning',
          groupName: 'Flood Warning',
          thresholdID: 'RES SFW All',
          thresholdName: 'RES SFW',
          shortName: 'RES SFW'
        }]
      const expectedNumberOfExceptionRows = 1
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has all rows with missing values', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'all-data-rows-missing-some-values.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedThresholdGroupsData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows)
    })

    it('should ignore rows that contains values exceeding a specified limit', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'one-row-has-data-over-specified-limits.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedThresholdGroupsData = [
        {
          groupID: 'Flood Warning',
          groupName: 'Flood Warning',
          thresholdID: 'RES SFW All',
          thresholdName: 'RES SFW',
          shortName: 'RES SFW'
        }]
      const expectedNumberOfExceptionRows = 0
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has no header row, only data rows', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'no-header-row.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedThresholdGroupsData = dummyData
      const expectedNumberOfExceptionRows = 1
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has a missing header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'missing-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedThresholdGroupsData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows)
    })

    it('should ignore a csv that has a misspelled header row', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'misspelled-headers.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedThresholdGroupsData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows)
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

      const expectedThresholdGroupsData = [
        {
          groupID: 'Flood Warning',
          groupName: 'Flood Warning',
          thresholdID: 'RES SFW All',
          thresholdName: 'RES SFW',
          shortName: 'RES SFW'
        },
        {
          groupID: 'Flood Warning',
          groupName: 'Flood Warning',
          thresholdID: 'RES FW All',
          thresholdName: 'RES FW',
          shortName: 'RES FW'
        }]
      const expectedNumberOfExceptionRows = 0
      const expectedServiceConfigurationUpdateNotification = false
      // Ensure a service configuration update is not detected.
      await commonCSVTestUtils.insertNonWorkflowRefreshRecords(-500)
      await commonWorkflowCSVTestUtils.insertWorkflowRefreshRecords()
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification)
    })

    it('should not refresh given a valid CSV file with null values in some of all row cells', async () => {
      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'empty-values-in-data-rows.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      const expectedThresholdGroupsData = dummyData
      const expectedNumberOfExceptionRows = 2
      await refreshThresholdGroupsDataAndCheckExpectedResults(mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows)
    })

    it('should throw an exception when the csv server is unavailable', async () => {
      const expectedError = new Error('connect ECONNREFUSED mockhost')
      fetch.mockImplementation(() => {
        throw new Error('connect ECONNREFUSED mockhost')
      })
      await expect(messageFunction(context, message)).rejects.toThrow(expectedError)
    })

    it('should throw an exception when the forecast location table is in use', async () => {
      // If the threshold groups table is being refreshed messages are eligible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.

      const mockResponseData = {
        statusCode: STATUS_CODE_200,
        filename: 'valid.csv',
        statusText: STATUS_TEXT_OK,
        contentType: TEXT_CSV
      }

      await lockThresholdGroupsTableAndCheckMessageCannotBeProcessed(mockResponseData)
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
      fetch.mockResolvedValue(mockResponse)

      const expectedData = dummyData
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error('No csv file detected')

      await expect(messageFunction(context, message)).rejects.toThrow(expectedError)
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
      fetch.mockResolvedValue(mockResponse)

      const expectedData = dummyData
      const expectedNumberOfExceptionRows = 0
      const expectedError = new Error('No csv file detected')

      await expect(messageFunction(context, message)).rejects.toThrow(expectedError)
      await checkExpectedResults(expectedData, expectedNumberOfExceptionRows)
    })
  })

  async function refreshThresholdGroupsDataAndCheckExpectedResults (mockResponseData, expectedThresholdGroupsData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification) {
    await mockFetchResponse(mockResponseData)
    await messageFunction(context, message) // calling actual function here
    await checkExpectedResults(expectedThresholdGroupsData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification)
  }

  async function mockFetchResponse (mockResponseData) {
    let mockResponse = {}
    mockResponse = {
      status: mockResponseData.statusCode,
      body: fs.createReadStream(`testing/function-tests/RefreshThresholdGroupsData/threshold_groups_files/${mockResponseData.filename}`),
      statusText: mockResponseData.statusText,
      headers: { 'Content-Type': mockResponseData.contentType },
      sendAsJson: false,
      url: '.csv'
    }
    fetch.mockResolvedValue(mockResponse)
  }

  async function checkExpectedResults (expectedThresholdGroupsData, expectedNumberOfExceptionRows, expectedServiceConfigurationUpdateNotification) {
    const result = await request.query(`
    select 
      count(*) 
    as 
      number
    from 
      fff_staging.threshold_groups
       `)
    const expectedNumberOfRows = expectedThresholdGroupsData.length

    expect(result.recordset[0].number).toBe(expectedNumberOfRows)
    context.log(`Live data row count: ${result.recordset[0].number}, test data row count: ${expectedNumberOfRows}`)

    if (expectedNumberOfRows > 0) {
      for (const row of expectedThresholdGroupsData) {
        const groupID = row.groupID
        const groupName = row.groupName
        const thresholdID = row.thresholdID
        const thresholdName = row.thresholdName
        const shortName = row.shortName

        const databaseResult = await request.query(`
          select 
            count(*) 
          as 
            number 
          from 
            fff_staging.threshold_groups
          where 
            GROUP_ID = '${groupID}' and GROUP_NAME = '${groupName}'
            and THRESHOLD_ID = '${thresholdID}' and THRESHOLD_NAME = '${thresholdName}' and SHORT_NAME = '${shortName}'
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

  async function lockThresholdGroupsTableAndCheckMessageCannotBeProcessed (mockResponseData) {
    let transaction
    const tableName = 'threshold_groups'
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(`
      insert into 
        fff_staging.${tableName} (GROUP_ID, GROUP_NAME, THRESHOLD_ID, THRESHOLD_NAME, SHORT_NAME) 
      values 
        ('Flood Warning', 'Flood Warning', 'RES SFW All', 'RES SFW', 'RES SFW')
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
