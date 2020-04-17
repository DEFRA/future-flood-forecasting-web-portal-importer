const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, preparedStatement, csvUrl, tableName, csvLoadData, partialTableUpdate, rowKeyCheckOverride) {
  try {
    const transaction = preparedStatement.parent
    const response = await fetch(csvUrl)
    const rows = await neatCsv(response.body)
    const recordCountResponse = rows.length
    const failedRows = []

    // do not refresh the table if the csv is empty.
    if (recordCountResponse > 0) {
      await new sql.Request(transaction).batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} ${partialTableUpdate.whereClause}`)

      let columnNames = ''
      let preparedStatementValues = ''

      // set up prepared statement inputs
      for (let input of csvLoadData) {
        columnNames = columnNames + `${input.tableColumnName}, `
        preparedStatementValues = preparedStatementValues + `@${input.tableColumnName}, `
        // set the input values up - limit to type
        await preparedStatement.input(input.tableColumnName, sql[input.tableColumnType])
      }

      columnNames = columnNames.slice(0, -2)
      preparedStatementValues = preparedStatementValues.slice(0, -2)

      // set up the query. values are input at execution - '@' tells prepared statement to expect input
      await preparedStatement.prepare(`insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} (${columnNames}) values (${preparedStatementValues})`)

      for (const row of rows) {
        let preparedStatementExecuteObject = {}

        try {
          // check all the values are present in the csv row, ignore incomplete rows
          let allRowKeysPresent
          for (let columnObject of csvLoadData) {
            row[`${columnObject.expectedCSVKey}`] ? allRowKeysPresent = true : allRowKeysPresent = false
            // allow an override to load csv rows with incoplete data
            if (rowKeyCheckOverride === true) {
              allRowKeysPresent = true
            }

            if (allRowKeysPresent === false) {
              break
            } else {
              preparedStatementExecuteObject[`${columnObject.tableColumnName}`] = row[`${columnObject.expectedCSVKey}`]
              // e.g preparedStatementExecuteObject = {
              // tableColumnName = csv row value for workflowId,
              // workflow_id = workflowA
              // Repeated for every column of the row
              // }
            }
          }
          if (allRowKeysPresent) {
            await preparedStatement.execute(preparedStatementExecuteObject)
          } else {
            context.log.warn(`A row is missing data.`)
            const failedRowInfo = {
              rowData: row,
              errorMessage: `A row is missing data.`,
              errorCode: `NA`
            }
            failedRows.push(failedRowInfo)
          }
        } catch (err) {
          context.log.warn(`An error has been found in a row.\nError : ${err}`)
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
    } else {
      // If the csv is empty then the file is essentially ignored
      context.log.warn(`No records detected - Aborting ${tableName} refresh.`)
    }
    const result = await new sql.Request(transaction).query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} ${partialTableUpdate.whereClause}`)
    context.log.info(`The ${tableName} table now contains ${result.recordset[0].number} new/updated records`)
    if (result.recordset[0].number === 0) {
      // If all the records in the csv were invalid, this query needs rolling back to avoid a blank database overwrite.
      context.log.warn('There were 0 new records to insert, a null database overwrite is not allowed. Rolling back non_display_group_workflow refresh.')
      await transaction.rollback()
      context.log.warn('Transaction rolled back.')
    }
    // Regardless of whether a rollback took place, all the failed rows are captured for loading into exceptions.
    context.log.error(`The ${tableName} csv loader failed to load ${failedRows.length} rows.`)
    return failedRows
  } catch (err) {
    context.log.error(`Refresh ${tableName} data failed: ${err}`)
    throw err
  }
}
