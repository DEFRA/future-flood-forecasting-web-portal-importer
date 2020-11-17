const sql = require('mssql')

module.exports = async function countTableRecords (context, transaction, tableName, countQuery) {
  const request = new sql.Request(transaction)
  const result = await request.query(countQuery)
  context.log.info(`The ${tableName} table contains ${result.recordset[0].number} records`)
  return result.recordset[0].number
}
