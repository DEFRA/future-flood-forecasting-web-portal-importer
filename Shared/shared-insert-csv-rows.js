const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, preparedStatement, csvUrl, tableName, functionSpecificData, partialTableUpdate) {
  try {
    const transaction = preparedStatement.parent
    const response = await fetch(csvUrl)
    if (response.status === 200 && response.headers[`Content-Type`] === 'text/csv') {
      const csvRows = await neatCsv(response.body)
      const csvRowCount = csvRows.length
      const failedcsvRows = []

      // do not refresh the table if the csv is empty.
      if (csvRowCount > 0) {
        if (partialTableUpdate.flag) {
          await new sql.Request(transaction).query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} ${partialTableUpdate.whereClause}`)
        } else {
          await new sql.Request(transaction).query(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName}`)
        }
        let columnNames = ''
        let preparedStatementValues = ''
        for (let columnObject of functionSpecificData) {
          // preparedStatement inputs
          columnNames = columnNames + `${columnObject.tableColumnName}, `
          preparedStatementValues = preparedStatementValues + `@${columnObject.tableColumnName}, ` // '@' values are input at execution.
          await preparedStatement.input(columnObject.tableColumnName, sql[columnObject.tableColumnType])
        }
        columnNames = columnNames.slice(0, -2)
        preparedStatementValues = preparedStatementValues.slice(0, -2)

        await preparedStatement.prepare(`insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} (${columnNames}) values (${preparedStatementValues})`)

        for (const row of csvRows) {
          let preparedStatementExecuteObject = {}
          try {
            // check all the expected values are present in the csv row and exclude incomplete csvRows.
            let allRowKeysPresent
            for (let columnObject of functionSpecificData) {
              row[`${columnObject.expectedCSVKey}`] ? allRowKeysPresent = true : allRowKeysPresent = false
              if (allRowKeysPresent === false) {
                break
              } else {
                preparedStatementExecuteObject[`${columnObject.tableColumnName}`] = row[`${columnObject.expectedCSVKey}`]
              }
            }
            if (allRowKeysPresent) {
              await preparedStatement.execute(preparedStatementExecuteObject)
            } else {
              context.log.warn(`row is missing data.`)
              const failedRowInfo = {
                rowData: row,
                errorMessage: `row is missing data.`,
                errorCode: `NA`
              }
              failedcsvRows.push(failedRowInfo)
            }
          } catch (err) {
            context.log.warn(`An error has been found in a row.\nError : ${err}`)
            const failedRowInfo = {
              rowData: row,
              errorMessage: err.message,
              errorCode: err.code
            }
            failedcsvRows.push(failedRowInfo)
          }
        }
        // Future requests will fail until the prepared statement is unprepared.
        await preparedStatement.unprepare()

        // Check updated table row count
        const result = await new sql.Request(transaction).query(`
        select 
          count(*) 
        as 
          number 
        from 
          ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.${tableName} ${partialTableUpdate.whereClause}`)
        context.log.info(`The ${tableName} table now contains ${result.recordset[0].number} new/updated records`)
        if (result.recordset[0].number === 0) {
          // If all the records in the csv were invalid, this query needs rolling back to avoid a blank database overwrite.
          context.log.warn('There were 0 new records to insert, a null database overwrite is not allowed. Rolling back refresh.')
          await transaction.rollback()
          context.log.warn('Transaction rolled back.')
        }
      } else {
        // If the csv is empty then the file is essentially ignored
        context.log.warn(`No records detected - Aborting ${tableName} refresh.`)
      }

      // Regardless of whether a rollback took place, all the failed csv rows are captured for loading into exceptions.
      context.log.warn(`The ${tableName} csv loader failed to load ${failedcsvRows.length} csvRows.`)
      return failedcsvRows
    } else {
      throw new Error(`No csv file detected`)
    }
  } catch (err) {
    context.log.error(`Refresh ${tableName} data failed: ${err}`)
    throw err
  }
}
