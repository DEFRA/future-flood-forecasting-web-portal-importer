module.exports = describe('Timeseries data deletion tests', () => {
  const Context = require('../testing/mocks/defaultContext')
  const Connection = require('../Shared/connection-pool')
  const timer = require('../testing/mocks/defaultTimer')
  const deleteFunction = require('./index')
  const moment = require('moment')
  const sql = require('mssql')

  let context
  const jestConnection = new Connection()
  const pool = jestConnection.pool
  const request = new sql.Request(pool)
  let hardLimit
  let softLimit

  describe('The delete expired staging timeseries data function:', () => {
    beforeAll(async (done) => {
      await pool.connect()
      done()
    })

    // Clear down all staging timeseries data tables. Due to referential integrity, query order must be preserved!
    beforeEach(async (done) => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for context needs creating for each test, jest.fn() mocks are contained within the Context class.
      context = new Context()
      delete process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT
      delete process.env.DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = 240
      process.env.DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT = 200
      hardLimit = parseInt(process.env['DELETE_EXPIRED_TIMESERIES_HARD_LIMIT'])
      softLimit = process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT'] ? parseInt(process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT']) : hardLimit
      await request.query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.timeseries_job`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
      done()
    })
    afterAll(async (done) => {
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.timeseries_job`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
      await pool.close()
      done()
    })
    it('should remove a record with a complete job status and with an import date older than the hard limit', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 6
      const testDescription = 'should remove a record with a complete job status and with an import date older than the hard limit'

      const expectedNumberofRows = 0

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should remove a record with a complete job status and with an import date older than the soft limit', async () => {
      const importDateStatus = 'exceedsSoft'
      const statusCode = 6
      const testDescription = 'should remove a record with a complete job status and with an import date older than the soft limit'

      const expectedNumberofRows = 0

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should remove a record with an incomplete job status and with an import date older than the hard limit', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 5
      const testDescription = 'should remove a record with an incomplete job status and with an import date older than the hard limit'

      const expectedNumberofRows = 0

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should NOT remove a record with an incomplete job status and with an import date older than the soft limit', async () => {
      const importDateStatus = 'exceedsSoft'
      const statusCode = 5
      const testDescription = 'should NOT remove a record with an incomplete job status and with an import date older than the soft limit'

      const expectedNumberofRows = 1

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
      await checkDescription(testDescription)
    })
    it('should remove a record with an incomplete job status and with an import date older than the soft limit, when soft limit equals hard limit', async () => {
      const importDateStatus = 'exceedsSoft' // also exceeds hard in this test
      const statusCode = 5
      const testDescription = 'should remove a record with an incomplete job status and with an import date older than the soft limit, when soft limit equals hard limit'

      process.env.DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT = process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT
      softLimit = hardLimit

      const expectedNumberofRows = 0

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should remove a record with a complete job status and with an import date older than the soft limit, when soft limit equals hard limit', async () => {
      const importDateStatus = 'exceedsSoft'
      const statusCode = 6
      const testDescription = 'should remove a record with a complete job status and with an import date older than the soft limit, when soft limit equals hard limit'
      const expectedNumberofRows = 0

      process.env.DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT = process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT
      softLimit = hardLimit

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should NOT remove a record with an incomplete job status and with an import date younger than the soft limit', async () => {
      const importDateStatus = 'activeDate'
      const statusCode = 5
      const testDescription = 'should NOT remove a record with an incomplete job status and with an import date younger than the soft limit'

      const expectedNumberofRows = 1

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
      await checkDescription(testDescription)
    })
    it('should NOT remove a record with a complete job status and with an import date younger than the soft limit', async () => {
      const importDateStatus = 'activeDate'
      const statusCode = 6
      const testDescription = 'should NOT remove a record with a complete job status and with an import date younger than the soft limit'

      const expectedNumberofRows = 1

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
      await checkDescription(testDescription)
    })
    it('Should be able to delete timeseries whilst another default level SELECT transaction is taking place on one of the tables involved', async () => {
      const expectedNumberofRows = 0
      await checkDeleteResolvesWithDefaultHeaderTableIsolationOnSelect(expectedNumberofRows)
    })
    it('Should NOT be able to delete timeseries whilst another default level INSERT transaction is taking place on one of the tables involved', async () => {
      const importDateStatus = 'exceedsHard'

      const importDate = await createImportDate(importDateStatus)
      await checkDeleteRejectsWithDefaultHeaderTableIsolationOnInsert(importDate)
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 5000)
    it('Should reject deletion if the DELETE_EXPIRED_TIMESERIES_HARD_LIMIT is not set', async () => {
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = null
      await expect(runTimerFunction()).rejects.toEqual(new Error('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT needs setting before timeseries can be removed.'))
    })
    it('Should reject deletion if the DELETE_EXPIRED_TIMESERIES_HARD_LIMIT is a string', async () => {
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = 'string'
      await expect(runTimerFunction()).rejects.toEqual(new Error('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT must be an integer greater than 0.'))
    })
    it('Should reject deletion if the DELETE_EXPIRED_TIMESERIES_HARD_LIMIT is 0 hours', async () => {
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = 0
      await expect(runTimerFunction()).rejects.toEqual(new Error('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT needs setting before timeseries can be removed.'))
    })
    it('should reject with a soft limit set higher than the hard limit', async () => {
      process.env.DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT = 51
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = 50

      await expect(runTimerFunction()).rejects.toEqual(new Error('DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT must be an integer and less than or equal to the hard-limit.'))
    })
    it('should reject if the soft-limit has been set as a string', async () => {
      process.env.DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT = 'eighty'
      await expect(runTimerFunction()).rejects.toEqual(new Error('DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT must be an integer and less than or equal to the hard-limit.'))
    })
  })

  async function createImportDate (importDateStatus) {
    let importDate
    switch (importDateStatus) {
      case 'activeDate':
        importDate = await moment.utc().toDate().toISOString()
        break
      case 'exceedsSoft':
        importDate = await moment.utc().subtract(parseInt(softLimit), 'hours').toDate().toISOString()
        break
      case 'exceedsHard':
        importDate = await moment.utc().subtract(parseInt(hardLimit), 'hours').toDate().toISOString()
        break
    }
    return importDate
  }

  async function runTimerFunction () {
    await deleteFunction(context, timer) // calling actual function here
  }

  async function insertRecordIntoTables (importDate, statusCode, testDescription) {
    // The importDate is created using the same limits (ENV VARs) that the function uses to calculate old data,
    // the function will look for anything older than the limit supplied (compared to current time).
    // As this insert happens first (current time is older in comparison to when the delete function runs),
    // the inserted data in tests will always be older. Date storage ISO 8601 allows this split seconds difference to be picked up.
    let query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
    declare @id2 uniqueidentifier
      set @id2 = newid()
    insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header (id, start_time, end_time, task_completion_time, task_run_id, workflow_id, import_time)
    values (@id1, cast('2017-01-24' as datetimeoffset),cast('2017-01-26' as datetimeoffset),cast('2017-01-25' as datetimeoffset),0,0,cast('${importDate}' as datetimeoffset))
    insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries (id, fews_data, fews_parameters,timeseries_header_id)
    values (@id2, 'data','parameters', @id1)
    insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.timeseries_job (timeseries_id, job_id, job_status, job_status_time, description)
    values (@id2, 78787878, ${statusCode}, cast('2017-01-28' as datetimeoffset), '${testDescription}')`
    query.replace(/"/g, "'")

    await request.query(query)
  }

  async function checkDeletionStatus (expectedLength) {
    const result = await request.query(`
    select r.description, h.import_time
      from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header h 
      inner join ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries t
        on t.timeseries_header_id = h.id
      inner join ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.timeseries_job r
        on r.timeseries_id = t.id
      order by import_time desc
  `)
    expect(result.recordset.length).toBe(expectedLength)
  }

  async function checkDescription (testDescription) {
    const result = await request.query(`
    select r.description
      from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header h 
      inner join ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries t
        on t.timeseries_header_id = h.id
      inner join ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.timeseries_job r
        on r.timeseries_id = t.id
      order by import_time desc
  `)
    expect(result.recordset[0].description).toBe(testDescription)
  }

  async function checkDeleteResolvesWithDefaultHeaderTableIsolationOnSelect (expectedLength) {
    let transaction
    try {
      transaction = new sql.Transaction(pool) // using Jest pool
      await transaction.begin(sql.ISOLATION_LEVEL.READ_COMMITTED) // the isolation level used by other transactions on the three tables concerned
      const newRequest = new sql.Request(transaction)

      let query = `select * from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`
      await newRequest.query(query)

      await expect(deleteFunction(context, timer)).resolves.toBe(undefined) // seperate request (outside the newly created transaction, out of the pool of available transactions)
      await checkDeletionStatus(expectedLength)
    } finally {
      if (transaction._aborted) {
        context.log.warn('The test transaction has been aborted.')
      } else {
        await transaction.rollback()
        context.log.warn('The test transaction has been rolled back.')
      }
    }
  }
  async function checkDeleteRejectsWithDefaultHeaderTableIsolationOnInsert (importDate) {
    let transaction
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.READ_COMMITTED) // the isolation level used by other transactions on the three tables concerned
      const newRequest = new sql.Request(transaction)
      let query = `
      declare @id1 uniqueidentifier set @id1 = newid()
      insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header (id, start_time, end_time, task_completion_time, task_run_id, workflow_id, import_time)
        values (@id1, cast('2017-01-24' as datetimeoffset),cast('2017-01-26' as datetimeoffset),cast('2017-01-25' as datetimeoffset),0,0,cast('${importDate}' as datetimeoffset))`
      query.replace(/"/g, "'")
      await newRequest.query(query)
      await expect(deleteFunction(context, timer)).rejects.toBeTimeoutError('timeseries_header') // seperate request (outside the newly created transaction, out of the pool of available transactions)
    } finally {
      if (transaction._aborted) {
        context.log.warn('The test transaction has been aborted.')
      } else {
        await transaction.rollback()
        context.log.warn('The test transaction has been rolled back.')
      }
    }
  }
})
