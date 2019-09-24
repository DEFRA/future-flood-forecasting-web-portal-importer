const { pool, sql } = require('./connection-pool')

module.exports = {
  doPreparedStatementInTransaction: async function (fn, context, ...args) {
    let transaction
    let preparedStatement
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin()
      preparedStatement = new sql.PreparedStatement(transaction)
      // Call the function that prepares and executes the prepared statement passing
      // through the arguments from the caller.
      return await fn(preparedStatement, ...args)
    } catch (err) {
      if (preparedStatement && preparedStatement.prepared) {
        await preparedStatement.unprepare()
        preparedStatement = null
      }
      await transaction.rollback()
      transaction = null
      throw err
    } finally {
      try {
        if (preparedStatement) {
          await preparedStatement.unprepare()
        }
        if (transaction) {
          await transaction.commit()
        }
      } catch (err) { context.log.error(err) }
    }
  }
}
