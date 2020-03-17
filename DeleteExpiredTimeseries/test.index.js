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

  const hardLimit = parseInt(process.env['DELETE_EXPIRED_TIMESERIES_HARD_LIMIT'])
  const softLimit = process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT'] ? parseInt(process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT']) : hardLimit

  describe('The refresh forecast location data function:', () => {
    beforeAll(() => {
      return pool.connect()
    })

    beforeEach(() => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
    })

    // Clear down all staging timeseries data tables (Order must be preserved)
    beforeEach(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.timeseries_job`)
    })
    beforeEach(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries`)
    })
    beforeEach(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header`)
    })
    afterAll(() => {
      return request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.timeseries_job`)
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
    it('Should be able to delete timeseries whilst another default level transaction is taking place on one of the tables involved', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 1
      const testDescription = 'Should be able to delete timeseries whilst another default level transaction is taking place on one of the tables involved'

      const expectedNumberofRows = 0

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await checkRunWithDefaultHeaderTableIsolation(expectedNumberofRows)
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
    let query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
    declare @id2 uniqueidentifier
      set @id2 = newid()
    insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries_header (id, start_time, end_time, task_completion_time, task_id, workflow_id, import_time)
    values (@id1, cast('2017-01-24' as datetime2),cast('2017-01-26' as datetime2),cast('2017-01-25' as datetime2),0,0,cast('${importDate}' as datetime2))
    insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries (id, fews_data, fews_parameters,timeseries_header_id)
    values (@id2, 'data','parameters', @id1)
    insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA']}.timeseries_job (timeseries_id, job_id, job_status, job_status_time, description)
    values (@id2, 78787878, ${statusCode}, cast('2017-01-28' as datetime2), '${testDescription}')`
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
  async function checkRunWithDefaultHeaderTableIsolation (expectedLength) {
    let transaction
    const tableName = 'timeseries_header'
    try {
      transaction = new sql.Transaction(pool) // using Jest pool
      await transaction.begin(null) // 'null' is the isolation level used by other transactions on the tables concerned
      const newRequest = new sql.Request(transaction)
      await newRequest.query(`
      select id, start_time from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName}
     `)
      await expect(deleteFunction(context, timer)).resolves.toBe(undefined) // seperate request (outside the transaction), using the same pool
      await checkDeletionStatus(expectedLength) // another seperate request usung the same pool
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
