const sql = require('mssql')

const deleteCSVStagingExceptionsQuery = `
-- csv exceptions to be deleted
delete
  cse
from
    fff_staging.csv_staging_exception cse
where 
    csv_source_file = @csvSourceFile
select 
  @@rowcount as deleted`

module.exports = async function deleteInactiveStagingExceptions (context, preparedStatement, csvSourceFile) {
  await preparedStatement.input('csvSourceFile', sql.NVarChar)
  await preparedStatement.prepare(deleteCSVStagingExceptionsQuery)
  const parameters = {
    csvSourceFile
  }
  const result = await preparedStatement.execute(parameters)
  context.log.info(`The 'RefreshCSV' function has deleted ${result.recordset[0].deleted} rows from the csv_staging_exception table, for the ${csvSourceFile} csv source file.`)
}
