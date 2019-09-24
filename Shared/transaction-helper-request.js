const { pool, sql } = require('./connection-pool')

module.exports = {
  doRequestInTransaction: async function (fn, context, ...args) {
    let transaction
    let request
    try {
      transaction = new sql.Transaction(pool)
      await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE)
      request = new sql.Request(transaction)
      // Call the function that prepares and executes the prepared statement passing
      // through the arguments from the caller.
      return await fn(request, context, ...args)
    } catch (err) {
      // context.log.error(err)
      await transaction.rollback()
      transaction = null
      throw err
    } finally {
      try {
        if (transaction) {
          await transaction.commit()
        }
      } catch (err) {}
    }
  }
}
