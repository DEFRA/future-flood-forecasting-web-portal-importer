const Connection = require('../Shared/connection-pool')
const sql = require('mssql')

module.exports = {
  doInTransaction: async function (fn, context, isolationLevel, ...args) {
    const connection = new Connection()
    const pool = connection.pool
    const request = new sql.Request(pool)

    let transaction
    let transactionRolledBack = false
    let preparedStatement

    try {
      // Begin the connection to the DB and ensure the connection pool is ready
      await pool.connect()
      await request.batch(`set lock_timeout ${process.env['SQLDB_LOCK_TIMEOUT'] || 6500};`)
      // The transaction is created immediately for use
      transaction = new sql.Transaction(pool)

      if (isolationLevel) {
        await transaction.begin(isolationLevel)
      } else {
        await transaction.begin()
      }
      preparedStatement = new sql.PreparedStatement(transaction)
      const transactionData = {
        preparedStatement: preparedStatement,
        transaction: transaction
      }
      // Call the function that prepares and executes the prepared statement passing
      // through the arguments from the caller.
      return await fn(transactionData, ...args)
    } catch (err) {
      if (preparedStatement && preparedStatement.prepared) {
        await preparedStatement.unprepare()
      }
      if (transaction) {
        await transaction.rollback()
        transactionRolledBack = true
      }
      throw err
    } finally {
      try {
        if (preparedStatement && preparedStatement.prepared) {
          await preparedStatement.unprepare()
        }
      } catch (err) { context.log.error(err) }
      try {
        if (transaction && !transactionRolledBack) {
          await transaction.commit()
        }
      } catch (err) { context.log.error(err) }
      try {
        if (pool) {
          await pool.close()
        }
      } catch (err) { context.log.error(err) }
    }
  }
}
