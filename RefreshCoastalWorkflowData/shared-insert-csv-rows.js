const { executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const createCSVStagingException = require('../Shared/create-csv-staging-exception')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, preparedStatement, csvUrl, tableName, csvLoadData) {
  try {
    const transaction = preparedStatement.parent
    const response = await fetch(`${process.env[csvUrl]}`)
    const rows = await neatCsv(response.body)
    const recordCountResponse = rows.length

    // Do not refresh the ignored workflow table if the csv is empty.
    if (recordCountResponse > 0) {
      await new sql.Request(transaction).batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName}`)

      const failedRows = []

      let columnNames
      let preparedStatementValues
      for (let input in csvLoadData) {
        columnNames.concat(`${input.tableColumnName},`)
        preparedStatementValues.concat(`@${input.tableColumnName},`)
        await preparedStatement.input(input.tableColumnName, input.tableColumnType)
        // e.g await preparedStatement.input('WORKFLOW_ID', sql.NVarChar)
      }
      await preparedStatement.prepare(`insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} ${columnNames} values ${preparedStatementValues}`)

      for (const row of rows) {
        // check all the expected row values are present in the csv row
        // Ignore rows in the CSV data that do not have entries for all columns.
        let preparedStatementExecuteObject = {}
        try {
          let allRowKeysPresent
          for (let object in csvLoadData) {
            row[`${object.tableColumnName}`] ? allRowKeysPresent = true : allRowKeysPresent = false
            if (allRowKeysPresent === false) {
              break
            } else {
              preparedStatementExecuteObject[`${object.tableColumnName}`] = row[`${object.tableColumnName}`]
            }
          }
          if (allRowKeysPresent) {
            await preparedStatement.execute(preparedStatementExecuteObject)
          } else {
            const failedRowInfo = {
              rowData: row,
              errorMessage: `A row is missing data.`,
              errorCode: `NA`
            }
            failedRows.push(failedRowInfo)
          }
        } catch (err) {
          context.log.warn(`an error has been found in a row with the WorkflowID: ${row.WorkflowID}.\n  Error : ${err}`)
          const failedRowInfo = {
            rowData: row,
            errorMessage: err.message,
            errorCode: err.code
          }
          failedRows.push(failedRowInfo)
        }
      }
      // Future requests will fail until the prepared statement is unprepared.
      await preparedStatement.unprepare()

      for (let i = 0; i < failedRows.length; i++) {
        await executePreparedStatementInTransaction(
          createCSVStagingException, // function
          context, // context
          transaction, // transaction
          `Ignored workflows`, // args - csv file
          failedRows[i].rowData, // args - row data
          failedRows[i].errorMessage // args - error description
        )
      }
      context.log.error(`The ignored workflow csv loader has ${failedRows.length} failed row inserts.`)
    } else {
      // If the csv is empty then the file is essentially ignored
      context.log.warn('No records detected - Aborting ignored_workflow refresh')
    }
    const result = await new sql.Request(transaction).query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow`)
    context.log.info(`The ignored workflow table contains ${result.recordset[0].number} records`)
    if (result.recordset[0].number === 0) {
      // If all the records in the csv were invalid, the function will overwrite records in the table with no new records
      // after the table has already been truncated. This function needs rolling back to avoid a blank database overwrite.
      context.log.warn('There are no new records to insert, rolling back ignored_workflow refresh')
      throw new Error('A null database overwrite is not allowed')
    }
  } catch (err) {
    context.log.error(`Refresh ignored_workflow data failed: ${err}`)
    throw err
  }
}
