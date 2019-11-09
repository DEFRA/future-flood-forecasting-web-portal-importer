// const { pool, sql, pooledConnect } = require('./connection-pool')
const Connection = require('../Shared/connection-pool')
const sql = require('mssql')
// const pooledConnect = connection.pooledConnect

module.exports = {
  doInTransaction: async function (fn, context, isolationLevel, ...args) {
    const connection = new Connection()
    const pool = connection.pool
    const pooledConnect = connection.pooledConnect
    const request = new sql.Request(pool)

    // Ensure the connection pool is ready
    await pooledConnect

    await request.batch(`set lock_timeout ${process.env['SQLDB_LOCK_TIMEOUT'] || 6500};`)
    let transaction
    let preparedStatement
    try {
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
      await transaction.rollback()
      throw err
    } finally {
      try {
        if (preparedStatement && preparedStatement.prepared) {
          await preparedStatement.unprepare()
        }
        if (transaction) {
          await transaction.commit()
        }
        // if (process.env.JEST_WORKER_ID === undefined) {
        await pool.close() // shuts all connections down manually (removes temp database issue)
        // }
      } catch (err) { }
    }
  }
}
