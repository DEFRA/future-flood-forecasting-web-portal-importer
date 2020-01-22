module.exports = describe('Tests for import timeseries ignored workflows', () => {
  const taskRunCompleteMessages = require('../testing/messages/task-run-complete/ignored-workflow-messages')
  const Context = require('../testing/mocks/defaultContext')
  const Connection = require('../Shared/connection-pool')
  const messageFunction = require('./index')
  const axios = require('axios')
  const sql = require('mssql')

  let context
  jest.mock('axios')

  const jestConnection = new Connection()
  const pool = jestConnection.pool
  const request = new sql.Request(pool)

  describe('Message processing for ignored workflows', () => {
    beforeAll(() => {
      return pool.connect()
    })

    beforeAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_display_group_workflow`)
    })

    beforeAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.fluvial_non_display_group_workflow`)
    })

    beforeAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow`)
    })

    beforeAll(() => {
      return request.batch(`
        insert into
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow (workflow_id)
        values
          ('Test_Ignored_Workflow_1'), ('Test_Ignored_Workflow_2')
      `)
    })

    beforeEach(() => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
    })

    beforeEach(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
    })

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow`)
    })

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
    })

    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
    })

    afterAll(() => {
      // Closing the DB connection allows Jest to exit successfully.
      return pool.close()
    })
    it('should ignore an ignored workflow', async () => {
      await processMessageAndCheckNoDataIsImported('ignoredForecast')
    })
    it('should throw an exception when the ignored_workflow table is being refreshed', async () => {
      // If the ignored_workflow table is being refreshed messages are elgible for replay a certain number of times
      // so check that an exception is thrown to facilitate this process.
      const mockResponse = {
        data: {
          key: 'Timeseries display groups data'
        }
      }
      await lockIgnoredWorkflowTableAndCheckMessageCannotBeProcessed('ignoredForecast', mockResponse)
      // Set the test timeout higher than the database request timeout.
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    it('should create a staging exception for an invalid message', async () => {
      await processMessageAndCheckStagingExceptionIsCreated('forecastWithoutApprovalStatus', 'Unable to extract task run approval status from message')
    })
  })

  async function processMessage (messageKey, mockResponses) {
    if (mockResponses) {
      let mock = axios.get
      for (const mockResponse of mockResponses) {
        mock = mock.mockReturnValueOnce(mockResponse)
      }
    }
    await messageFunction(context, JSON.stringify(taskRunCompleteMessages[messageKey]))
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

  async function lockIgnoredWorkflowTableAndCheckMessageCannotBeProcessed (messageKey, mockResponse) {
    let transaction
    const tableName = 'ignored_workflow'
    try {
      // Lock the ignored_workflow table and then try and process the message.
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      const request = new sql.Request(transaction)
      await request.batch(`
      insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} (workflow_id) 
      values 
      ('dummyWorkflow')
    `)
      await expect(processMessage(messageKey, [mockResponse])).rejects.toBeTimeoutError(tableName)
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
