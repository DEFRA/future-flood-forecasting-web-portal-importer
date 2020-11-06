const sql = require('mssql')

module.exports = async function countTableRecords (context, transaction, tableName) {
  const request = new sql.Request(transaction)
  await request.input('table', sql.NVarChar, tableName)
  const result = await request.query(`EXEC('select count(*) as number from ' + @table)`)
  context.log.info(`The ${tableName} table contains ${result.recordset[0].number} records`)
  return result.recordset[0].number
}
