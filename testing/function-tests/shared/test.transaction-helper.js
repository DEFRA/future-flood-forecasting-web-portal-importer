const sql = require('mssql')
const { doInTransaction, executePreparedStatementInTransaction } = require('../../../Shared/transaction-helper')
const ConnectionPool = require('../../../Shared/connection-pool')
const Context = require('../mocks/defaultContext')

const jestConnectionPool = new ConnectionPool()
const pool = jestConnectionPool.pool
const errorMessage = 'ERROR'

let context

module.exports = describe('Test transaction helper', () => {
  describe('The transaction helper', () => {
    beforeAll(async () => {
      await pool.connect()
    })
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
    })

    afterAll(async () => {
      // Closing the DB connection allows Jest to exit successfully.
      await pool.close()
    })
    it('should throw an exception following a failure to commit a transaction', done => {
      jest.isolateModules(async () => {
        try {
          jest.spyOn(sql, 'Request').mockImplementation(() => {
            return {
              batch: jest.fn(),
              query: jest.fn()
            }
          })
          jest.spyOn(sql, 'Transaction').mockImplementation(() => {
            return {
              begin: jest.fn(),
              commit: jest.fn().mockImplementation(() => {
                throw new Error(errorMessage)
              })
            }
          })
          await expect(doInTransaction({ fn: () => {}, context, errorMessage: 'Error' })).rejects.toThrow(errorMessage)
          done()
        } catch (err) {
          done(err)
        }
      })
    })
    it('should throw an exception following a failure to unprepare a PreparedStatement', done => {
      jest.isolateModules(async () => {
        try {
          jest.spyOn(sql, 'PreparedStatement').mockImplementation(() => {
            return {
              prepare: jest.fn(),
              prepared: true,
              unprepare: jest.fn().mockImplementation(() => {
                throw new Error(errorMessage)
              })
            }
          })
          jest.spyOn(sql, 'Request').mockImplementation(() => {
            return {
              batch: jest.fn(),
              query: jest.fn()
            }
          })
          jest.spyOn(sql, 'Transaction').mockImplementation(() => {
            return {
              begin: jest.fn(),
              commit: jest.fn()
            }
          })
          await expect(doInTransaction({ fn: testExecutePreparedStatementInTransaction, context, errorMessage: 'Error' })).rejects.toThrow(errorMessage)
          done()
        } catch (err) {
          done(err)
        }
      })
    })
    it('should throw an exception if a transaction is not present following a failure', done => {
      jest.isolateModules(async () => {
        try {
          jest.spyOn(sql, 'Request').mockImplementation(() => {
            return {
              batch: jest.fn(),
              query: jest.fn()
            }
          })
          jest.spyOn(sql, 'Transaction').mockImplementation(() => {})
          await expect(doInTransaction({ fn: () => {}, context, errorMessage: 'Error' })).rejects.toThrow('transaction.begin is not a function')
          done()
        } catch (err) {
          done(err)
        }
      })
    })
    it('should not attempt to commit an aborted transaction', done => {
      jest.isolateModules(async () => {
        try {
          jest.spyOn(sql, 'Request').mockImplementation(() => {
            return {
              batch: jest.fn(),
              query: jest.fn()
            }
          })
          jest.spyOn(sql, 'Transaction').mockImplementation(() => {
            return {
              _aborted: true,
              begin: jest.fn(),
              commit: jest.fn().mockImplementation(() => {
                throw new Error('COMMIT ERROR')
              })
            }
          })
          await expect(doInTransaction({ fn: () => { throw new Error(errorMessage) }, context, errorMessage: 'Error' })).rejects.toThrow(errorMessage)
          done()
        } catch (err) {
          done(err)
        }
      })
    })
    it('should not attempt to commit a transaction for which rollback has been requested', done => {
      jest.isolateModules(async () => {
        try {
          jest.spyOn(sql, 'Request').mockImplementation(() => {
            return {
              batch: jest.fn(),
              query: jest.fn()
            }
          })
          jest.spyOn(sql, 'Transaction').mockImplementation(() => {
            return {
              _rollbackRequested: true,
              begin: jest.fn(),
              commit: jest.fn().mockImplementation(() => {
                throw new Error('COMMIT ERROR')
              })
            }
          })
          await expect(doInTransaction({ fn: () => { throw new Error(errorMessage) }, context, errorMessage: 'Error' })).rejects.toThrow(errorMessage)
          done()
        } catch (err) {
          done(err)
        }
      })
    })
  })
})

async function testExecutePreparedStatementInTransaction (transaction, context) {
  await executePreparedStatementInTransaction(() => {}, context, transaction)
}
