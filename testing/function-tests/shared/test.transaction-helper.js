import sql from 'mssql'
import { doInTransaction, executePreparedStatementInTransaction } from '../../../Shared/transaction-helper.js'
import ConnectionPool from '../../../Shared/connection-pool.js'
import Context from '../mocks/defaultContext.js'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'

const viConnectionPool = new ConnectionPool()
const pool = viConnectionPool.pool
const errorMessage = 'ERROR'

let context

export const transactionHelperTests = () => describe('Test transaction helper', () => {
  describe('The transaction helper', () => {
    beforeAll(async () => {
      await pool.connect()
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Vitest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
      await import('mssql')
    })

    afterEach(async () => {
      vi.doUnmock('mssql')
    })

    afterAll(async () => {
      // Closing the DB connection allows Vitest to exit successfully.
      await pool.close()
    })
    it('should throw an exception following a failure to commit a transaction', async () => {
      vi.spyOn(sql, 'Request').mockImplementation(() => {
        return {
          batch: vi.fn(),
          query: vi.fn()
        }
      })
      vi.spyOn(sql, 'Transaction').mockImplementation(() => {
        return {
          begin: vi.fn(),
          commit: vi.fn().mockImplementation(() => {
            throw new Error(errorMessage)
          })
        }
      })
      await expect(doInTransaction({ fn: () => {}, context, errorMessage: 'Error' })).rejects.toThrow(errorMessage)
    })
    it('should throw an exception following a failure to unprepare a PreparedStatement', async () => {
      vi.spyOn(sql, 'PreparedStatement').mockImplementation(() => {
        return {
          prepare: vi.fn(),
          prepared: true,
          unprepare: vi.fn().mockImplementation(() => {
            throw new Error(errorMessage)
          })
        }
      })
      vi.spyOn(sql, 'Request').mockImplementation(() => {
        return {
          batch: vi.fn(),
          query: vi.fn()
        }
      })
      vi.spyOn(sql, 'Transaction').mockImplementation(() => {
        return {
          begin: vi.fn(),
          commit: vi.fn()
        }
      })
      await expect(doInTransaction({ fn: testExecutePreparedStatementInTransaction, context, errorMessage: 'Error' })).rejects.toThrow(errorMessage)
    })
    it('should throw an exception if a transaction is not present following a failure', async () => {
      vi.spyOn(sql, 'Request').mockImplementation(() => {
        return {
          batch: vi.fn(),
          query: vi.fn()
        }
      })
      vi.spyOn(sql, 'Transaction').mockImplementation(() => {
        return {
          acquire: vi.fn()
        }
      })
      await expect(doInTransaction({ fn: () => {}, context, errorMessage: 'Error' })).rejects.toThrow('transaction.begin is not a function')
    })
    it('should not attempt to commit an aborted transaction', async () => {
      vi.spyOn(sql, 'Request').mockImplementation(() => {
        return {
          batch: vi.fn(),
          query: vi.fn()
        }
      })
      vi.spyOn(sql, 'Transaction').mockImplementation(() => {
        return {
          _aborted: true,
          begin: vi.fn(),
          commit: vi.fn().mockImplementation(() => {
            throw new Error('COMMIT ERROR')
          })
        }
      })
      await expect(doInTransaction({ fn: () => { throw new Error(errorMessage) }, context, errorMessage: 'Error' })).rejects.toThrow(errorMessage)
    })
    it('should not attempt to commit a transaction for which rollback has been requested', async () => {
      vi.spyOn(sql, 'Request').mockImplementation(() => {
        return {
          batch: vi.fn(),
          query: vi.fn()
        }
      })
      vi.spyOn(sql, 'Transaction').mockImplementation(() => {
        return {
          _rollbackRequested: true,
          begin: vi.fn(),
          commit: vi.fn().mockImplementation(() => {
            throw new Error('COMMIT ERROR')
          })
        }
      })
      await expect(doInTransaction({ fn: () => { throw new Error(errorMessage) }, context, errorMessage: 'Error' })).rejects.toThrow(errorMessage)
    })
  })
})

async function testExecutePreparedStatementInTransaction (transaction, context) {
  await executePreparedStatementInTransaction(() => {}, context, transaction)
}
