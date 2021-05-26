const ConnectionPool = require('../Shared/connection-pool')
const { logger } = require('defra-logging-facade')
const sql = require('mssql')
const lockValue = parseInt(process.env.SQLDB_LOCK_TIMEOUT)
const connectionPoolClosedMessage = 'Connection pool is closed'

let connectionPool
let pool

module.exports = {
  closeConnectionPool: async function () {
    await closeConnectionPoolInternal()
  },
  doInTransaction: async function (config, ...args) {
    const context = config.context
    if (connectionPool) {
      let transaction

      try {
        transaction = await beginTransaction(context, config.isolationLevel)

        // Call the function to be executed in the transaction passing
        // through the transaction, context and arguments from the caller.
        context.log(`Connection pool: size=${pool.size}, available=${pool.available}, borrowed=${pool.borrowed} pending=${pool.pending}`)
        return await config.fn(transaction, context, ...args)
      } catch (err) {
        try {
          context.log.error(`Transaction failed: ${config.errorMessage} ${err}`)
          if (transaction) {
            if (transaction._aborted) {
              context.log.warn('The transaction has been aborted.')
            } else if (transaction._rollbackRequested) {
              await resetConnectionAndEndTransaction(context, transaction)
              context.log.warn('Transaction rollback has been requested.')
            } else {
              await resetConnectionAndEndTransaction(context, transaction, transaction.rollback.bind(transaction))
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
            await resetConnectionAndEndTransaction(context, transaction, transaction.commit.bind(transaction))
          }
        } catch (err) { context.log.error(`Transaction-helper cleanup error: '${err.message}'.`) }
      }
    } else {
      throw new Error(`${connectionPoolClosedMessage} - please restart the function app`)
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
    logger.info(connectionPoolClosedMessage)
  }
}

async function beginTransaction (context, isolationLevel) {
  await pool.connect()
  const transaction = new sql.Transaction(pool)

  if (isolationLevel) {
    await transaction.begin(isolationLevel)
  } else {
    await transaction.begin()
  }

  const request = new sql.Request(transaction)

  // The setting of SET LOCK_TIMEOUT is set at execute or run time and not at parse time. Parameterisation is not available so the input is sanitised
  await request.batch(`set lock_timeout ${(Number.isInteger(lockValue) && Number(lockValue) > 2000) ? lockValue : 6500}`)
  return Promise.resolve(transaction)
}

async function resetConnectionAndEndTransaction (context, transaction, endTransactionFunction) {
  // Restore the default transaction isolation level before returning the connection to the pool.
  // NOTE - The reset function of the pooled connection (see http://tediousjs.github.io/tedious/api-connection.html#function_reset)
  // is not used as this appears to cause intermittent connection state problems. Individual functions are responsible
  // for performing operations that are part of returning a pooled connection to its initial state (such as dropping local temporary tables).
  const request = new sql.Request(transaction)
  await request.batch('set transaction isolation level read committed')
  // Commit or rollback the transaction
  await endTransactionFunction()
}

async function initialiseConnectionPool () {
  const shutdownEvents = ['beforeExit', 'SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGILL', 'SIGTRAP', 'SIGABRT', 'SIGBUS', 'SIGFPE', 'SIGUSR1', 'SIGSEGV', 'SIGUSR2', 'SIGTERM']
  const pooledConnections = []
  connectionPool = new ConnectionPool()
  pool = connectionPool.pool
  await pool.connect()

  sql.on('error', err => {
    logger.error(err)
    throw err
  })

  // Ensure the connection pool contains the minimum number of connections before any messages are processed.
  // https://github.com/Vincit/tarn.js/issues/1
  for (let index = 0; index < pool.pool.min; index++) {
    pooledConnections[index] = await pool.acquire()
  }

  for (let index = 0; index < pooledConnections.length; index++) {
    await pool.release(pooledConnections[index])
  }

  // Add shutdown event handlers.
  for (const shutdownEvent of shutdownEvents) {
    process.on(shutdownEvent, await closeConnectionPoolInternal)
  }

  logger.info(`Initialised connection pool with ${pool.pool.min} connection(s)`)
}

// Ensure the connection pool is initialised before any messages are processed.
(async () => {
  await initialiseConnectionPool()
})()
