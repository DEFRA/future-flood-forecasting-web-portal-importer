const deleteFunction = require('../../../DeleteExpiredTimeseries/index')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')
const timer = require('../mocks/defaultTimer')
const moment = require('moment')
const sql = require('mssql')
const { getEnvironmentVariableAsAbsoluteInteger } = require('../../../Shared/utils')

module.exports = describe('Timeseries data deletion tests', () => {
  let context
  const jestConnectionPool = new ConnectionPool()
  const pool = jestConnectionPool.pool
  const request = new sql.Request(pool)
  let hardLimit
  let withinLimit // the withinLimit (only has context in the tests) is a set amount of time less than the hard limit

  describe('The delete expired staging timeseries data function:', () => {
    // there are 3 possible scenarios of data to be deleted:
    // 1) Data row exists in header-timeseries-reporting (and possible exceptions for partial loading)
    // 2) Data row exists in header and exceptions for failed loads
    // 3) Data row exists in header alone if data not loaded for header (in no data is returned or data out of date)
    beforeAll(async () => {
      await pool.connect()
      await request.batch('set lock_timeout 5000;')
    })

    // Clear down all staging timeseries data tables. Due to referential integrity, query order must be preserved!
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for context needs creating for each test, jest.fn() mocks are contained within the Context class.
      context = new Context()
      delete process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = '240'
      hardLimit = parseInt(process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT)
      withinLimit = parseInt(process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT) - 1
      // The order of deletion is sensitive to referential integrity
      await request.query('delete from fff_reporting.timeseries_job')
      await request.query('delete from fff_staging.inactive_timeseries_staging_exception')
      await request.batch('delete from fff_staging.timeseries')
      await request.batch('delete from fff_staging.timeseries_staging_exception')
      await request.batch('delete from fff_staging.inactive_staging_exception')
      await request.batch('delete from fff_staging.staging_exception')
      await request.batch('delete from fff_staging.timeseries_header')
    })
    afterAll(async () => {
      await request.query('delete from fff_reporting.timeseries_job')
      await request.query('delete from fff_staging.inactive_timeseries_staging_exception')
      await request.batch('delete from fff_staging.timeseries')
      await request.batch('delete from fff_staging.timeseries_staging_exception')
      await request.batch('delete from fff_staging.inactive_staging_exception')
      await request.batch('delete from fff_staging.staging_exception')
      await request.batch('delete from fff_staging.timeseries_header')
      await pool.close()
    })
    it('should delete a record with a complete job status and with an import date older than the hard limit', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 6
      const testDescription = 'should delete a record with a complete job status and with an import date older than the hard limit'

      const expectedNumberofRows = 0

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should NOT delete a record with a complete job status and with an import date younger than the hard limit', async () => {
      const importDateStatus = 'withinLimit'
      const statusCode = 6
      const testDescription = 'should NOT delete a record with a complete job status and with an import date younger than the hard limit'

      const expectedNumberofRows = 2

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
      await checkDescription(testDescription)
    })
    it('should delete a record with an incomplete job status and with an import date older than the hard limit', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 5
      const testDescription = 'should delete a record with an incomplete job status and with an import date older than the hard limit'

      const expectedNumberofRows = 0

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('Should be able to delete timeseries whilst another default level SELECT transaction is taking place on one of the tables involved', async () => {
      const expectedNumberofRows = 0
      await checkDeleteResolvesWithDefaultHeaderTableIsolationOnSelect(expectedNumberofRows)
    })
    it('Should NOT be able to delete timeseries whilst another default level INSERT transaction with locks is taking place on one of the tables involved', async () => {
      const importDateStatus = 'exceedsHard'

      const importDate = await createImportDate(importDateStatus)
      await checkDeleteRejectsWithDefaultHeaderTableIsolationOnInsert(importDate)
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 5000)
    it('Should prevent deletion if the DELETE_EXPIRED_TIMESERIES_HARD_LIMIT is not set', async () => {
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = null
      await expect(runTimerFunction()).rejects.toEqual(new Error('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT needs setting before timeseries can be removed.'))
    })
    it('Should prevent deletion if the DELETE_EXPIRED_TIMESERIES_HARD_LIMIT is a string', async () => {
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = 'string'
      await expect(runTimerFunction()).rejects.toEqual(new Error('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT must be an integer greater than 0.'))
    })
    it('Should prevent deletion if the DELETE_EXPIRED_TIMESERIES_HARD_LIMIT is 0 hours', async () => {
      process.env.DELETE_EXPIRED_TIMESERIES_HARD_LIMIT = '0'
      await expect(runTimerFunction()).rejects.toEqual(new Error('DELETE_EXPIRED_TIMESERIES_HARD_LIMIT must be an integer greater than 0.'))
    })
    it('A separate transaction WITH isolation lock hint should NOT be able to select rows from the reporting table whilst the delete transaction is taking place on those rows', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 6
      const testDescription = 'A separate transaction (with lock hint) should NOT be able to select rows from the reporting table whilst the delete transaction is taking place on those rows'

      const importDate = await createImportDate(importDateStatus)
      const isolationHintSet = true
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await checkSelectRejectsWithDeleteInProgress(isolationHintSet)
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 35000)
    it('Check for snapshot isolation (Azure DB default). Check select rejects with no snapshot and no table hint with delete in progress (will use default Read Committed isolation), else check select is successful when delete is in progress with snapshot isolation ON', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 6
      const testDescription = { rowsAffected: [2] }

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)

      const snapshotBoolean = await checkSnapshotIsolationOn()
      if (!snapshotBoolean) {
        // no snapshot isolation with no isolation lock hint and default READ COMMITTED isolation (SQL server default)
        const isolationHintSet = false
        await checkSelectRejectsWithDeleteInProgress(isolationHintSet)
      } else {
        await checkSnapshotIsolationSelectSucceedsWithDeleteInProgress(testDescription)
      }
    }, parseInt(process.env.SQLTESTDB_REQUEST_TIMEOUT || 15000) + 35000)
    it('should NOT delete records only existing in timeseries_header and timeseries_staging_exception that have a younger import date than the hard limit', async () => {
      const importDateStatus = 'withinLimit'
      const testDescription = 'should NOT delete a record only existing in timeseries_header and timeseries_staging_exception that is younger than the hard limit'

      const expectedNumberofRows = 1

      const importDate = await createImportDate(importDateStatus)
      await insertTimeseriesExceptionRecordIntoTables(importDate, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should delete records only existing in timeseries_header and timeseries_staging_exception that have an older import date than the hard limit', async () => {
      const importDateStatus = 'exceedsHard'
      const testDescription = 'should delete a record only existing in timeseries_header and timeseries_staging_exception that is older than the hard limit'

      const expectedNumberofRows = 0

      const importDate = await createImportDate(importDateStatus)
      await insertTimeseriesExceptionRecordIntoTables(importDate, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should NOT delete a record only existing in timeseries_header that has a younger import date than the hard limit', async () => {
      const importDateStatus = 'withinLimit'
      const testDescription = 'should NOT delete a record only existing in timeseries_header that is younger than the hard limit'

      const expectedNumberofRows = 1

      const importDate = await createImportDate(importDateStatus)
      await insertHeaderRecordIntoTables(importDate, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should delete a record only existing in timeseries_header that has an older import date than the hard limit', async () => {
      const importDateStatus = 'exceedsHard'
      const testDescription = 'should delete a record only existing in timeseries_header that is older than the hard limit'

      const expectedNumberofRows = 0

      const importDate = await createImportDate(importDateStatus)
      await insertHeaderRecordIntoTables(importDate, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should delete an inactive timeseries exception that has an older import date than the hard limit', async () => {
      const importDateStatus = 'exceedsHard'
      const expectedNumberofRows = 0
      const importDate = await createImportDate(importDateStatus)

      await insertTimeseriesExceptionRecordIntoTables(importDate)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should NOT delete an inactive timeseries exception with an import date younger than the hard limit date', async () => {
      const importDateStatus = 'withinLimit'
      const expectedNumberofRows = 1
      const importDate = await createImportDate(importDateStatus)

      await insertTimeseriesExceptionRecordIntoTables(importDate)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should delete all records for a single header row given a batch size smaller than the number of rows in the reporting table/timeseries table/exceptions table/inactive exceptions table for an import date older than the hard limit', async () => {
      const importDateStatus = 'exceedsHard'
      const statusCode = 6
      const testDescription = 'should delete all records for a single header row given a batch size smaller than the number of rows in the reporting table/timeseries table/exceptions table/inactive exceptions table for an import date older than the hard limit'

      const expectedNumberofRows = 0

      process.env.TIMESERIES_DELETE_BATCH_SIZE = '1'

      const importDate = await createImportDate(importDateStatus)
      await insertRecordIntoTables(importDate, statusCode, testDescription)
      await runTimerFunction()
      await checkDeletionStatus(expectedNumberofRows)
    })
    it('should delete a staging exception record (and the associated inactive-staging-exception) and with an import date older than the hard limit', async () => {
      const importDateStatus = 'exceedsHard'

      const expectedNumberofRows = 0

      const exceptionTime = await createImportDate(importDateStatus)
      await insertInactiveStagingExceptionRecordIntoTables(exceptionTime)
      await runTimerFunction()
      await checkStagingExceptionDeletionStatus(expectedNumberofRows)
    })
    it('should NOT delete a staging exception record (and the associated inactive-staging-exception) with an import date younger than the hard limit', async () => {
      const importDateStatus = 'withinLimit'

      const expectedNumberofRows = 1
      const expectedDescription = 'this record has an associated inactive exception'

      const exceptionTime = await createImportDate(importDateStatus)
      await insertInactiveStagingExceptionRecordIntoTables(exceptionTime)
      await runTimerFunction()
      await checkStagingExceptionDeletionStatus(expectedNumberofRows, expectedDescription)
    })
    it('should NOT delete a staging exception record (with no associated-inactive-staging exception) with an import date younger than the hard limit date', async () => {
      const importDateStatus = 'withinLimit'

      const expectedNumberofRows = 1
      const expectedDescription = 'this record has no associated inactive exception'

      const exceptionTime = await createImportDate(importDateStatus)
      await insertStagingExceptionRecordIntoTables(exceptionTime)
      await runTimerFunction()
      await checkStagingExceptionDeletionStatus(expectedNumberofRows, expectedDescription)
    })
    it('should delete a staging exception record (with no associated-inactive-staging exception) with an import date older than the hard limit date', async () => {
      const importDateStatus = 'exceedsHard'

      const expectedNumberofRows = 0

      const exceptionTime = await createImportDate(importDateStatus)
      await insertStagingExceptionRecordIntoTables(exceptionTime)
      await runTimerFunction()
      await checkStagingExceptionDeletionStatus(expectedNumberofRows)
    })
  })

  async function createImportDate (importDateStatus) {
    let importDate
    switch (importDateStatus) {
      case 'activeDate':
        importDate = await moment.utc().toDate().toISOString()
        break
      case 'withinLimit':
        importDate = await moment.utc().subtract(parseInt(withinLimit), 'hours').toDate().toISOString()
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
    // the import date was created earlier in the test and reflects the limit that the record will exceed in this test case
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      declare @id2 uniqueidentifier
      set @id2 = newid()
      declare @id3 uniqueidentifier
      set @id3 = newid()
      insert into fff_staging.timeseries_header (id, task_completion_time, task_run_id, workflow_id, import_time, message)
        values (@id1, cast('2017-01-24' as datetimeoffset),0,0,cast('${importDate}' as datetimeoffset), '{"key": "value"}')
      insert into fff_staging.timeseries_staging_exception (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description)
        values (@id3, 'error_plot', 'P', 1, 'C', 'error_plot_fews_parameters', '{"taskRunId": 0, "plotId": "error_plot"}', @id1, 'Error plot text')
      insert into fff_staging.timeseries (id, fews_data, fews_parameters, timeseries_header_id)
        values (@id2, compress('data'),'parameters', @id1)
      insert into fff_reporting.timeseries_job (timeseries_id, job_status, job_status_time, description)
        values (@id2, ${statusCode}, cast('2017-01-28' as datetimeoffset), '${testDescription}'),
        (@id2, ${statusCode}, cast('2017-01-28' as datetimeoffset), '${testDescription}')`
    query.replace(/"/g, "'")

    await request.query(query)
  }

  async function insertTimeseriesExceptionRecordIntoTables (importDate) {
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      declare @id2 uniqueidentifier
      set @id2 = newid()
      insert into fff_staging.timeseries_header (id, task_completion_time, task_run_id, workflow_id, import_time, message)
        values (@id1, cast('2017-01-24' as datetimeoffset),0,0,cast('${importDate}' as datetimeoffset), '{"key": "value"}')
      insert into fff_staging.timeseries_staging_exception (id, source_id, source_type, csv_error, csv_type, fews_parameters, payload, timeseries_header_id, description)
        values (@id2, 'error_plot', 'P', 1, 'C', 'error_plot_fews_parameters', '{"taskRunId": 0, "plotId": "error_plot"}', @id1, 'Error plot text')
      insert into fff_staging.inactive_timeseries_staging_exception (timeseries_staging_exception_id, deactivation_time)
        values (@id2, cast('2017-01-25' as datetimeoffset))`
    query.replace(/"/g, "'")

    await request.query(query)
  }

  async function insertInactiveStagingExceptionRecordIntoTables (exceptionTime) {
    try {
      const query = `
      declare @inactive_staging_exception uniqueidentifier
        set @inactive_staging_exception = newid()
      insert into
        fff_staging.staging_exception (id, task_run_id, payload, description, exception_time, source_function, workflow_id)
      values
        (@inactive_staging_exception, 0, 'payload', 'this record has an associated inactive exception', cast('${exceptionTime}' as datetimeoffset), 'P', 'workflow1')
      insert into
        fff_staging.inactive_staging_exception (staging_exception_id, deactivation_time)
      values
        (@inactive_staging_exception, cast('${exceptionTime}' as datetimeoffset))`
      query.replace(/"/g, "'")

      await request.query(query)
    } catch (err) {
      console.log(err)
    }
  }

  async function insertStagingExceptionRecordIntoTables (exceptionTime) {
    const query = `
      declare @active_staging_exception uniqueidentifier
        set @active_staging_exception = newid()
      insert into
        fff_staging.staging_exception (id, task_run_id, payload, description, exception_time, source_function, workflow_id)
      values
        (@active_staging_exception, 0, 'payload', 'this record has no associated inactive exception', cast('${exceptionTime}' as datetimeoffset), 'P', 'workflow1')`
    query.replace(/"/g, "'")

    await request.query(query)
  }

  async function insertHeaderRecordIntoTables (importDate, statusCode, testDescription) {
    const query = `
      declare @id1 uniqueidentifier
      set @id1 = newid()
      insert into
        fff_staging.timeseries_header (id, task_completion_time, task_run_id, workflow_id, import_time, message)
      values
        (@id1, cast('2017-01-24' as datetimeoffset),0,0,cast('${importDate}' as datetimeoffset), '{"key": "value"}')`
    query.replace(/"/g, "'")

    await request.query(query)
  }

  async function checkDeletionStatus (expectedLength) {
    const result = await request.query(`
    select
        r.description,
        h.import_time
    from
      fff_staging.timeseries_header h
      left join fff_staging.timeseries t on t.timeseries_header_id = h.id
      left join fff_reporting.timeseries_job r on r.timeseries_id = t.id
      left join fff_staging.timeseries_staging_exception tse on tse.timeseries_header_id = h.id
      left join fff_staging.inactive_timeseries_staging_exception itse on itse.timeseries_staging_exception_id = tse.id
    order by
      h.import_time desc
    `)

    expect(result.recordset.length).toBe(expectedLength)
  }

  async function checkStagingExceptionDeletionStatus (expectedLength, expectedDescription) {
    const result = await request.query(`
    select
        se.id,
        se.description,
        ise.id
    from
      fff_staging.staging_exception se
      full outer join fff_staging.inactive_staging_exception ise on ise.staging_exception_id = se.id
    order by
      se.exception_time desc
    `)

    expect(result.recordset.length).toBe(expectedLength)
    if (expectedDescription) {
      expect(result.recordset[0].description).toBe(expectedDescription)
    }
  }
  async function checkDescription (testDescription) {
    const result = await request.query(`
    select r.description
      from fff_staging.timeseries_header h
      inner join fff_staging.timeseries t
        on t.timeseries_header_id = h.id
      inner join fff_reporting.timeseries_job r
        on r.timeseries_id = t.id
      order by h.import_time desc
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

      const query = `
      select
        *
      from
        fff_staging.timeseries_header`
      await newRequest.query(query)

      await expect(deleteFunction(context, timer)).resolves.toBe(undefined) // separate request (outside the newly created transaction, out of the pool of available transactions)
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
      const query = `
      declare @id1 uniqueidentifier set @id1 = newid()
      insert into
        fff_staging.timeseries_header with (tablock holdlock) (id, task_completion_time, task_run_id, workflow_id, import_time, message)
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
  async function checkSnapshotIsolationSelectSucceedsWithDeleteInProgress (testDescription) {
    let transaction1
    let transaction2
    try {
      transaction1 = new sql.Transaction(pool)
      await transaction1.begin(sql.ISOLATION_LEVEL.READ_COMMITTED) // current delete level
      const newRequest = new sql.Request(transaction1)
      const query =
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
      process.env.SQLDB_LOCK_TIMEOUT ? lockTimeoutValue = getEnvironmentVariableAsAbsoluteInteger(process.env.SQLDB_LOCK_TIMEOUT) : lockTimeoutValue = 6500
      const query2 =
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
      context.log.warn('The test transaction have been rolled back.')
    }
  }
  async function checkSelectRejectsWithDeleteInProgress (isolationHintSet) {
    let transaction1
    let transaction2
    try {
      transaction1 = new sql.Transaction(pool)
      await transaction1.begin()
      const newRequest = new sql.Request(transaction1)
      const query =
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
      process.env.SQLDB_LOCK_TIMEOUT ? lockTimeoutValue = getEnvironmentVariableAsAbsoluteInteger(process.env.SQLDB_LOCK_TIMEOUT) : lockTimeoutValue = 6500
      const query2 =
        `set lock_timeout ${lockTimeoutValue}
         select * from fff_reporting.TIMESERIES_JOB ${isolationHintSet ? 'with (readcommittedlock)' : ''}
        where JOB_STATUS = 6`
      await expect(newRequest2.query(query2)).rejects.toBeTimeoutError('TIMESERIES_JOB') // separate request (outside the newly created transaction, out of the pool of available transactions)
    } finally {
      if (transaction1 && !transaction1._aborted) {
        await transaction1.rollback()
      }
      if (transaction2 && !transaction2._aborted) {
        await transaction2.rollback()
      }
      context.log.warn('The test transactions have been rolled back.')
    }
  }
})
