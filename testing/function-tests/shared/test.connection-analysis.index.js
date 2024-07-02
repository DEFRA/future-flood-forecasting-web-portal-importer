import { doInTransaction } from '../../../Shared/transaction-helper.js'
import Context from '../mocks/defaultContext.js'
import sql from 'mssql'
import { beforeEach, describe, expect, it } from 'vitest'

export const sharedConnectionTests = () => describe('Test shared connection', () => {
  let context

  describe('Transaction helper', () => {
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Vitest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
    })

    it('should throw an exception when the set timeout value is an injection script', async () => {
      process.env.SQLDB_LOCK_TIMEOUT = 'delete from fff_staging.IGNORED_WORKFLOW'
      const isolationLevel = sql.ISOLATION_LEVEL.SERIALIZABLE
      const lockValue = await doInTransaction({ fn: getLockTimeout, context, errorMessage: 'The test failed with the following error:' }, isolationLevel)
      expect(lockValue).toEqual(6500)
    })
  })
  async function getLockTimeout (transaction, context) {
    let lockTimeoutValue = await new sql.Request(transaction).query('SELECT @@LOCK_TIMEOUT AS [LockTimeout];')
    lockTimeoutValue = lockTimeoutValue.recordset[0].LockTimeout
    return lockTimeoutValue
  }
})
