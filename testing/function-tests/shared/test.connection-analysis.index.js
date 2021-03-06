const { doInTransaction } = require('../../../Shared/transaction-helper')
const Context = require('../mocks/defaultContext')
const sql = require('mssql')

module.exports = describe('Test shared connection', () => {
  let context

  describe('Transaction helper', () => {
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
    })

    it('should throw an exception when the set timeout value is an injection script', async () => {
      process.env.SQLDB_LOCK_TIMEOUT = 'delete from fff_staging.IGNORED_WORKFLOW'
      const isolationLevel = sql.ISOLATION_LEVEL.SERIALIZABLE
      const lockVaue = await doInTransaction({ fn: getLockTimeout, context, errorMessage: 'The test failed with the following error:' }, isolationLevel)

      await expect(lockVaue).toEqual(6500)
    })
  })
  async function getLockTimeout (transaction, context) {
    let lockTimeoutValue = await new sql.Request(transaction).query('SELECT @@LOCK_TIMEOUT AS [LockTimeout];')
    lockTimeoutValue = lockTimeoutValue.recordset[0].LockTimeout
    return lockTimeoutValue
  }
})
