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
    beforeAll(async () => {
      await pool.connect()
      await request.batch(`set lock_timeout 5000;`)
    })

    // Clear down all staging timeseries data tables. Due to referential integrity, query order must be preserved!
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for context needs creating for each test, jest.fn() mocks are contained within the Context class.
      context = new Context()
      delete process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT
      delete process.env.DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = 240
      process.env.DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT = 200
      hardLimit = parseInt(process.env['DELETE_EXPIRED_TIMESERIES_HARD_LIMIT'])
      softLimit = process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT'] ? parseInt(process.env['DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT']) : hardLimit
      await request.query(`delete from fff_reporting.timeseries_job`)
      await request.batch(`delete from fff_staging.timeseries`)
      await request.batch(`delete from fff_staging.timeseries_header`)
    })
    afterAll(async () => {
      await request.batch(`delete from fff_reporting.timeseries_job`)
      await request.batch(`delete from fff_staging.timeseries`)
      await request.batch(`delete from fff_staging.timeseries_header`)
      await pool.close()
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
    it('A seperate transaction WITH isolation lock hint should NOT be able to select rows from the reporting table whilst the delete transaction is taking place on those rows', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 6
      const testDescription = 'A seperate transaction (with lock hint) should NOT be able to select rows from the reporting table whilst the delete transaction is taking place on those rows'

      const importDate = await createImportDate(importDateStatus)
      const isolationHintSet = true
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await checkSelectRejectsWithDeleteInProgress(isolationHintSet)
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 35000)
    it('Check for snapshot isolation (Azure DB default). Check select rejects with no snapshot and no table hint with delete in progress (will use default ReadCommited isolation), else check select is successful when delete is in progress with snapshot isolation ON', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 6
      const testDescription = { rowsAffected: [1] }

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)

      const snapshotBoolean = await checkSnapshotIsolationOn()
      if (!snapshotBoolean) {
        // no snapshot isolation with no isolation lock hint and default READ COMMITED isolation (SQL server default)
        let isolationHintSet = false
        await checkSelectRejectsWithDeleteInProgress(isolationHintSet)
      } else {
        await checkDefaultSelectSucceedsWithDeleteInProgress(testDescription)
      }
    }, parseInt(process.env['SQLTESTDB_REQUEST_TIMEOUT'] || 15000) + 35000)
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
    insert into fff_staging.timeseries_header (id, task_completion_time, task_run_id, workflow_id, import_time, message)
    values (@id1, cast('2017-01-24' as datetimeoffset),0,0,cast('${importDate}' as datetimeoffset), '{"key": "value"}')
    insert into fff_staging.timeseries (id, fews_data, fews_parameters,timeseries_header_id)
    values (@id2, compress('data'),'parameters', @id1)
    insert into fff_reporting.timeseries_job (timeseries_id, job_id, job_status, job_status_time, description)
    values (@id2, 78787878, ${statusCode}, cast('2017-01-28' as datetimeoffset), '${testDescription}')`
    query.replace(/"/g, "'")

    await request.query(query)
  }

  async function checkDeletionStatus (expectedLength) {
    const result = await request.query(`
    select r.description, h.import_time
      from fff_staging.timeseries_header h
      inner join fff_staging.timeseries t
        on t.timeseries_header_id = h.id
      inner join fff_reporting.timeseries_job r
        on r.timeseries_id = t.id
      order by import_time desc
  `)
    expect(result.recordset.length).toBe(expectedLength)
  }

  async function checkDescription (testDescription) {
    const result = await request.query(`
    select r.description
      from fff_staging.timeseries_header h
      inner join fff_staging.timeseries t
        on t.timeseries_header_id = h.id
      inner join fff_reporting.timeseries_job r
        on r.timeseries_id = t.id
      order by import_time desc
  `)
    expect(result.recordset[0].description).toBe(testDescription)
  }

  async function checkSnapshotIsolationOn () {
    const result = await request.query(`
    SELECT name
    , is_read_committed_snapshot_on
    FROM sys.databases
    WHERE name = (SELECT DB_NAME())
  `)
    if (result.recordset[0] && result.recordset[0].is_read_committed_snapshot_on) {
      expect(result.recordset[0].is_read_committed_snapshot_on).toBe(true)
      return true
    } else {
      return false
    }
  }

  async function checkDeleteResolvesWithDefaultHeaderTableIsolationOnSelect (expectedLength) {
    let transaction
    try {
      transaction = new sql.Transaction(pool) // using Jest pool
      await transaction.begin(sql.ISOLATION_LEVEL.READ_COMMITTED) // the isolation level used by other transactions on the three tables concerned
      const newRequest = new sql.Request(transaction)

      let query = `
      select 
        * 
      from 
        fff_staging.timeseries_header`
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
      insert into 
        fff_staging.timeseries_header (id, task_completion_time, task_run_id, workflow_id, import_time, message)
      values 
        (@id1, cast('2017-01-24' as datetimeoffset),0,0,cast('${importDate}' as datetimeoffset), '{"key": "value"}')`
      query.replace(/"/g, "'")
      await newRequest.query(query)
      await expect(deleteFunction(context, timer)).rejects.toBeTimeoutError('timeseries_header')
    } finally {
      if (transaction._aborted) {
        context.log.warn('The test transaction has been aborted.')
      } else {
        await transaction.rollback()
        context.log.warn('The test transaction has been rolled back.')
      }
    }
  }
  async function checkDefaultSelectSucceedsWithDeleteInProgress (testDescription) {
    let transaction1
    let transaction2
    try {
      transaction1 = new sql.Transaction(pool)
      await transaction1.begin(sql.ISOLATION_LEVEL.READ_COMMITTED) // current delete level
      const newRequest = new sql.Request(transaction1)
      let query =
        `delete
        from fff_reporting.TIMESERIES_JOB
        where JOB_STATUS = 6
      `
      await newRequest.query(query)

      transaction2 = new sql.Transaction(pool)
      await transaction2.begin()
      const newRequest2 = new sql.Request(transaction2)
      // Copy the lock timeout period
      let lockTimeoutValue
      process.env['SQLDB_LOCK_TIMEOUT'] ? lockTimeoutValue = process.env['SQLDB_LOCK_TIMEOUT'] : lockTimeoutValue = 6500
      let query2 =
        `set lock_timeout ${lockTimeoutValue}
         select * from fff_reporting.TIMESERIES_JOB
        where JOB_STATUS = 6`
      await expect(newRequest2.query(query2)).resolves.toMatchObject(testDescription)
    } finally {
      if (transaction1 && !transaction1._aborted) {
        await transaction1.rollback()
      }
      if (transaction2 && !transaction2._aborted) {
        await transaction2.rollback()
      }
      context.log.warn('The test transactiona have been rolled back.')
    }
  }
  async function checkSelectRejectsWithDeleteInProgress (isolationHintSet) {
    let transaction1
    let transaction2
    try {
      transaction1 = new sql.Transaction(pool)
      await transaction1.begin()
      const newRequest = new sql.Request(transaction1)
      let query =
        `delete
        from fff_reporting.TIMESERIES_JOB
        where JOB_STATUS = 6
      `
      await newRequest.query(query)

      transaction2 = new sql.Transaction(pool)
      await transaction2.begin()
      const newRequest2 = new sql.Request(transaction2)
      // Copy the lock timeout period
      let lockTimeoutValue
      process.env['SQLDB_LOCK_TIMEOUT'] ? lockTimeoutValue = process.env['SQLDB_LOCK_TIMEOUT'] : lockTimeoutValue = 6500
      let query2 =
        `set lock_timeout ${lockTimeoutValue}
         select * from fff_reporting.TIMESERIES_JOB ${isolationHintSet ? 'with (readcommittedlock)' : ''}
        where JOB_STATUS = 6`
      await expect(newRequest2.query(query2)).rejects.toBeTimeoutError('TIMESERIES_JOB') // seperate request (outside the newly created transaction, out of the pool of available transactions)
    } finally {
      if (transaction1 && !transaction1._aborted) {
        await transaction1.rollback()
      }
      if (transaction2 && !transaction2._aborted) {
        await transaction2.rollback()
      }
      context.log.warn('The test transactiona have been rolled back.')
    }
  }
})
