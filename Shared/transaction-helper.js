['beforeExit', 'SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGILL', 'SIGTRAP', 'SIGABRT', 'SIGBUS', 'SIGFPE', 'SIGUSR1', 'SIGSEGV', 'SIGUSR2', 'SIGTERM']
  .forEach(event => process.on(event, closeConnectionPoolInternal))

const ConnectionPool = require('../Shared/connection-pool')
const sql = require('mssql')
const { promisify } = require('util')

let connectionPool
let pool

module.exports = {
  closeConnectionPool: async function () {
    await closeConnectionPoolInternal()
  },
  doInTransaction: async function (fn, context, errorMessage, isolationLevel, ...args) {
    connectionPool = connectionPool || new ConnectionPool()
    pool = pool || connectionPool.pool

    const request = new sql.Request(pool)

    let transaction

    try {
      sql.on('error', err => {
        context.log.error(err)
        throw err
      })
      // Begin the connection to the DB and ensure the connection pool is ready
      await pool.connect()

      // Set the lock timeout period for the connection
      const lockValue = parseInt(process.env.SQLDB_LOCK_TIMEOUT)
      // The setting of SET LOCK_TIMEOUT is set at execute or run time and not at parse time. Parameterisation is not available so the input is sanitised
      // A batched request is utilised to ensure the timeout is executed on requests within the pool
      await request.batch(`set lock_timeout ${(Number.isInteger(lockValue) && Number(lockValue) > 2000) ? lockValue : 6500}`)

      // The transaction is created immediately for use
      transaction = new sql.Transaction(pool)

      if (isolationLevel) {
        await transaction.begin(isolationLevel)
      } else {
        await transaction.begin()
      }

      // Call the function to be executed in the transaction passing
      // through the transaction, context and arguments from the caller.
      return await fn(transaction, context, ...args)
    } catch (err) {
      try {
        context.log.error(`Transaction failed: ${errorMessage} ${err}`)
        if (transaction) {
          if (transaction._aborted) {
            context.log.warn('The transaction has been aborted.')
          } else if (transaction._rollbackRequested) {
            await endTransactionAndResetConnectionIfPossible(context, transaction)
            context.log.warn('Transaction rollback has been requested.')
          } else {
            await endTransactionAndResetConnectionIfPossible(context, transaction, transaction.rollback.bind(transaction))
            context.log.warn('The transaction has been rolled back.')
          }
        } else {
          context.log.error('No transaction to commit or rollback')
        }
      } catch (err) {
        context.log.error(`Transaction-helper cleanup error: '${err.message}'.`)
      }
      throw err
    } finally {
      try {
        if (transaction && !transaction._aborted && !transaction._rollbackRequested) {
          await endTransactionAndResetConnectionIfPossible(context, transaction, transaction.commit.bind(transaction))
        }
      } catch (err) { context.log.error(`Transaction-helper cleanup error: '${err.message}'.`) }
    }
  },
  executePreparedStatementInTransaction: async function (fn, context, transaction, ...args) {
    let preparedStatement
    try {
      preparedStatement = new sql.PreparedStatement(transaction)
      // Call the function that prepares and executes the prepared statement passing
      // through the arguments from the caller.
      return await fn(context, preparedStatement, ...args)
    } catch (err) {
      context.log.error(`${fn.name} - PreparedStatement Transaction-helper error: '${err.message}'.`)
      throw err
    } finally {
      try {
        if (preparedStatement && preparedStatement.prepared) {
          await preparedStatement.unprepare()
        }
      } catch (err) { context.log.error(`${fn.name} - PreparedStatement Transaction-helper error: '${err.message}'.`) }
    }
  }
}

async function closeConnectionPoolInternal () {
  if (pool) {
    await pool.close()
    connectionPool = undefined
    pool = undefined
  }
}

async function endTransactionAndResetConnectionIfPossible (context, transaction, endTransactionFunction) {
  if (transaction && transaction._acquiredConnection) {
    const promisifiedReset = promisify(transaction._acquiredConnection.reset.bind(transaction._acquiredConnection))
    if (endTransactionFunction) {
      // Restore the default transaction isolation level as a connection reset does not do this.
      const request = new sql.Request(transaction)
      await request.batch('set transaction isolation level read committed')
      // Commit or rollback the transaction before the pooled connection is reset as reset causes the transaction to be aborted.
      await endTransactionFunction()
    }
    // Reset the pooled connection to ensure temporary tables are cleared (see https://github.com/tediousjs/tedious/issues/85).
    await promisifiedReset()
    context.log('Connection has been reset')
  } else {
    context.warn('Unable to reset connection as it has released')
  }
}
