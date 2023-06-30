import { executePreparedStatementInTransaction } from '../../transaction-helper.js'
import sql from 'mssql'

export default async function (context, transaction, csvStagingExceptionData) {
  await executePreparedStatementInTransaction(createCsvStagingException, context, transaction, csvStagingExceptionData)
}

async function createCsvStagingException (context, preparedStatement, exceptionData) {
  try {
    await preparedStatement.input('sourceFile', sql.NVarChar)
    await preparedStatement.input('rowData', sql.NVarChar)
    await preparedStatement.input('description', sql.NVarChar)

    await preparedStatement.prepare(`
      insert into
        fff_staging.csv_staging_exception (csv_source_file, row_data, description)
      values
       (@sourceFile, @rowData, @description)
    `)

    const parameters = {
      sourceFile: exceptionData.sourceFile,
      rowData: JSON.stringify(exceptionData.rowData),
      description: exceptionData.description
    }

    await preparedStatement.execute(parameters)
  } catch (err) {
    context.log.error(err)
    throw err
  }
}
