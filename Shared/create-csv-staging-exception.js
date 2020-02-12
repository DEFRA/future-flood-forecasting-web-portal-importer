const sql = require('mssql')

module.exports = async function (context, preparedStatement, sourceFile, rowData, description) {
  try {
    await preparedStatement.input('sourceFile', sql.NVarChar)
    await preparedStatement.input('rowData', sql.NVarChar)
    await preparedStatement.input('description', sql.NVarChar)

    await preparedStatement.prepare(`
      insert into
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.csv_staging_exception (csv_source_file, row_data, description)
      values
       (@sourceFile, @rowData, @description)
    `)

    const parameters = {
      sourceFile: sourceFile,
      rowData: JSON.stringify(rowData),
      description: description
    }

    await preparedStatement.execute(parameters)
  } catch (err) {
    context.log.error(err)
    throw err
  }
}
