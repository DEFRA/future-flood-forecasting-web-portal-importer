const { doInTransaction, executePreparedStatementInTransaction } = require('./transaction-helper')
const loadExceptions = require('./failed-csv-load-handler/load-csv-exceptions')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, refreshData) {
  // Transaction 1
  // Refresh with a serializable isolation level so that refresh is prevented if the table is in use.
  // If the table is in use and table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refreshInTransaction, context, `The ${refreshData.type} refresh has failed with the following error:`, sql.ISOLATION_LEVEL.SERIALIZABLE, refreshData)

  // Transaction 2
  if (refreshData.failedRows.length > 0) {
    await doInTransaction(loadExceptions, context, `The ${refreshData.type} exception load has failed with the following error:`, sql.ISOLATION_LEVEL.SERIALIZABLE, refreshData.type, refreshData.failedRows)
  } else {
    context.log.info(`There were no csv exceptions during load.`)
  }
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}

async function refreshInTransaction (transaction, context, refreshData) {
  if (refreshData.preOperation) {
    await refreshData.preOperation(transaction, context)
  }

  await executePreparedStatementInTransaction(refreshInternal, context, transaction, refreshData)

  if (refreshData.postOperation) {
    await refreshData.postOperation(transaction, context)
  }
}

async function refreshInternal (context, preparedStatement, refreshData) {
  try {
    const transaction = preparedStatement.parent
    const response = await fetch(refreshData.csvUrl)
    if (response.status === 200 && response.url.includes('.csv')) {
      const csvRows = await neatCsv(response.body)
      const csvRowCount = csvRows.length
      const failedCsvRows = []

      // do not refresh the table if the csv is empty.
      if (csvRowCount > 0) {
        if (refreshData.partialTableUpdate.flag) {
          await new sql.Request(transaction).query(`
          delete 
          from
            fff_staging.${refreshData.tableName} ${refreshData.partialTableUpdate.whereClause}`)
        } else {
          await new sql.Request(transaction).query(`
          delete
          from 
            fff_staging.${refreshData.tableName}`)
        }
        let columnNames = ''
        let preparedStatementValues = ''
        for (let columnObject of refreshData.functionSpecificData) {
          // preparedStatement inputs
          columnNames = columnNames + `${columnObject.tableColumnName}, `
          preparedStatementValues = preparedStatementValues + `@${columnObject.tableColumnName}, ` // '@' values are input at execution.
          await preparedStatement.input(columnObject.tableColumnName, sql[columnObject.tableColumnType])
        }
        columnNames = columnNames.slice(0, -2)
        preparedStatementValues = preparedStatementValues.slice(0, -2)

        await preparedStatement.prepare(`
        insert into 
          fff_staging.${refreshData.tableName} (${columnNames})
        values 
          (${preparedStatementValues})`)

        for (const row of csvRows) {
          let preparedStatementExecuteObject = {}
          try {
            // check all the expected values are present in the csv row and exclude incomplete csvRows.
            let rowError = false
            for (let columnObject of refreshData.functionSpecificData) {
              if (refreshData.keyInteregator) {
                let keyPass = await refreshData.keyInteregator(columnObject.expectedCSVKey, row[`${columnObject.expectedCSVKey}`])
                if (keyPass === false) {
                  rowError = true
                  break
                }
              }
              // If the row-key contains data OR there is an override set to continue with row-key null value.
              if (row[`${columnObject.expectedCSVKey}`] || columnObject.nullValueOverride === true) {
                if (columnObject.preprocessor) {
                  preparedStatementExecuteObject[`${columnObject.tableColumnName}`] = columnObject.preprocessor(row[`${columnObject.expectedCSVKey}`])
                } else {
                  preparedStatementExecuteObject[`${columnObject.tableColumnName}`] = row[`${columnObject.expectedCSVKey}`]
                }
              } else {
                rowError = true
                break
              }
            }
            if (rowError) {
              context.log.warn(`row is missing data.`)
              const failedRowInfo = {
                rowData: row,
                errorMessage: `row is missing data.`,
                errorCode: `NA`
              }
              failedCsvRows.push(failedRowInfo)
            } else {
              await preparedStatement.execute(preparedStatementExecuteObject)
            }
          } catch (err) {
            context.log.warn(`An error has been found in a row.\nError : ${err}`)
            const failedRowInfo = {
              rowData: row,
              errorMessage: err.message,
              errorCode: err.code
            }
            failedCsvRows.push(failedRowInfo)
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
          fff_staging.${refreshData.tableName} ${refreshData.partialTableUpdate.whereClause}`)
        context.log.info(`The ${refreshData.tableName} table now contains ${result.recordset[0].number} new/updated records`)
        if (result.recordset[0].number === 0) {
          // If all the records in the csv were invalid, this query needs rolling back to avoid a blank database overwrite.
          context.log.warn('There were 0 new records to insert, a null database overwrite is not allowed. Rolling back refresh.')
          await transaction.rollback()
          context.log.warn('Transaction rolled back.')
        }
      } else {
        // If the csv is empty then the file is essentially ignored
        context.log.warn(`No records detected - Aborting ${refreshData.tableName} refresh.`)
      }

      // Regardless of whether a rollback took place, all the failed csv rows are captured for loading into exceptions.
      context.log.warn(`The ${refreshData.tableName} csv loader failed to load ${failedCsvRows.length} csvRows.`)
      refreshData.failedRows = failedCsvRows
    } else {
      throw new Error(`No csv file detected`)
    }
  } catch (err) {
    context.log.error(`Refresh ${refreshData.tableName} data failed: ${err}`)
    throw err
  }
}
