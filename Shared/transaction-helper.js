const { pool, sql } = require('./connection-pool')

module.exports = {
  doInTransaction: async function (fn, context, isolationLevel, ...args) {
    let request = new sql.Request(pool)
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
      } catch (err) {}
    }
  }
}
