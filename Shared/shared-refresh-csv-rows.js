const deleteCSVStagingExceptions = require('../Shared/failed-csv-load-handler/delete-csv-staging-exception')
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
  await doInTransaction(refreshInTransaction, context, `The ${refreshData.csvSourceFile} refresh has failed with the following error:`, sql.ISOLATION_LEVEL.SERIALIZABLE, refreshData)

  // Transaction 2
  if (refreshData.failedRows.length > 0) {
    await doInTransaction(loadExceptions, context, `The ${refreshData.csvSourceFile} exception load has failed with the following error:`, sql.ISOLATION_LEVEL.SERIALIZABLE, refreshData.csvSourceFile, refreshData.failedRows)
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

  if (!transaction._rollbackRequested) {
    if (refreshData.postOperation) {
      await refreshData.postOperation(transaction, context)
    }
    // remove the outdated csv staging exceptions for this csv csvSourceFile
    await executePreparedStatementInTransaction(deleteCSVStagingExceptions, context, transaction, refreshData.csvSourceFile)
    if (refreshData.workflowRefreshCsvType) {
      await executePreparedStatementInTransaction(updateWorkflowRefreshTable, context, transaction, refreshData)
    }
  }
}

async function updateWorkflowRefreshTable (context, preparedStatement, refreshData) {
  await preparedStatement.input('csvType', sql.NVarChar)

  await preparedStatement.prepare(`
    merge fff_staging.workflow_refresh with (holdlock) as target
    using (values (@csvType, getutcdate())) as source (csv_type, refresh_time)
    on (target.csv_type = source.csv_type)
    when matched then
      update set target.refresh_time = source.refresh_time
    when not matched then
      insert (csv_type, refresh_time)
        values (csv_type, refresh_time);
  `)

  const parameters = {
    csvType: refreshData.workflowRefreshCsvType
  }

  await preparedStatement.execute(parameters)
}

async function refreshInternal (context, preparedStatement, refreshData) {
  try {
    const transaction = preparedStatement.parent
    let configAuthorization = process.env['CONFIG_AUTHORIZATION']

    const refreshDataConfig = {
      method: 'get',
      headers: configAuthorization ? { Authorization: 'token ' + configAuthorization } : {}
    }

    configAuthorization = ''
    const response = await fetch(refreshData.csvUrl, refreshDataConfig)

    if (response.status === 200 && response.url.includes('.csv')) {
      const csvRows = await neatCsv(response.body)
      const csvRowCount = csvRows.length
      const failedCsvRows = []

      // do not refresh the table if the csv is empty.
      if (csvRowCount > 0) {
        await new sql.Request(transaction).query(refreshData.deleteStatement)

        for (const columnObject of refreshData.functionSpecificData) {
          if (columnObject.tableColumnType === 'Decimal') {
            await preparedStatement.input(columnObject.tableColumnName, sql.Decimal(columnObject.precision, columnObject.scale))
          } else {
            await preparedStatement.input(columnObject.tableColumnName, sql[`${columnObject.tableColumnType}`])
          }
        }

        await preparedStatement.prepare(refreshData.insertPreparedStatement)

        for (const row of csvRows) {
          const preparedStatementExecuteObject = {}
          try {
            // check all the expected values are present in the csv row and exclude incomplete csvRows.
            let rowError = false
            for (const columnObject of refreshData.functionSpecificData) {
              if (refreshData.keyInteregator) {
                const keyPass = await refreshData.keyInteregator(columnObject.expectedCSVKey, row[`${columnObject.expectedCSVKey}`])
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

        const result = await new sql.Request(transaction).query(refreshData.countStatement)
        context.log.info(`The ${refreshData.csvSourceFile} table now contains ${result.recordset[0].number} new/updated records`)
        if (result.recordset[0].number === 0) {
          // If all the records in the csv were invalid, this query needs rolling back to avoid a blank database overwrite.
          context.log.warn('There were 0 new records to insert, a null database overwrite is not allowed. Rolling back refresh.')
          await transaction.rollback()
          context.log.warn('Transaction rolled back.')
        }
      } else {
        // If the csv is empty then the file is essentially ignored
        context.log.warn(`No records detected - Aborting ${refreshData.csvSourceFile} refresh.`)
      }

      // Regardless of whether a rollback took place, all the failed csv rows are captured for loading into exceptions.
      context.log.warn(`The ${refreshData.csvSourceFile} csv loader failed to load ${failedCsvRows.length} csvRows.`)
      refreshData.failedRows = failedCsvRows
    } else {
      throw new Error(`No csv file detected`)
    }
  } catch (err) {
    context.log.error(`Refresh ${refreshData.csvSourceFile} data failed: ${err}`)
    throw err
  }
}
