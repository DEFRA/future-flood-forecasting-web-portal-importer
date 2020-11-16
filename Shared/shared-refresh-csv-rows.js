const deleteCsvStagingExceptions = require('../Shared/failed-csv-load-handler/delete-csv-staging-exception')
const { doInTransaction, executePreparedStatementInTransaction } = require('./transaction-helper')
const loadExceptions = require('./failed-csv-load-handler/load-csv-exceptions')
const replayEligibleStagingExceptions = require('./message-replay/replay-eligible-staging-exceptions')
const replayEligibleTimeseriesStagingExceptions = require('./message-replay/replay-eligible-timeseries-staging-exceptions')
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
  if (refreshData.failedCsvRows.length > 0) {
    await doInTransaction(loadExceptions, context, `The ${refreshData.csvSourceFile} exception load has failed with the following error:`, sql.ISOLATION_LEVEL.SERIALIZABLE, refreshData.csvSourceFile, refreshData.failedCsvRows)
  } else {
    context.log.info(`There were no csv exceptions during load.`)
  }
  // context.done() not required as the async function returns the desired result, there is no output binding to be activated.
}

async function refreshInTransaction (transaction, context, refreshData) {
  context.bindings.processFewsEventCode = []
  context.bindings.importFromFews = []

  if (refreshData.preOperation) {
    // A post operation involves further processing of the csv data after initial loading into the SQL database
    await refreshData.preOperation(transaction, context)
  }

  await executePreparedStatementInTransaction(refreshInternal, context, transaction, refreshData)

  if (!transaction._rollbackRequested) {
    if (refreshData.postOperation) {
      await refreshData.postOperation(transaction, context)
    }
    // remove the outdated csv staging exceptions for this csv csvSourceFile
    await executePreparedStatementInTransaction(deleteCsvStagingExceptions, context, transaction, refreshData.csvSourceFile)
    if (refreshData.workflowRefreshCsvType) {
      const replayData = {
        csvType: refreshData.workflowRefreshCsvType,
        transaction: transaction
      }
      await executePreparedStatementInTransaction(updateWorkflowRefreshTable, context, transaction, refreshData)

      // Attempt to replay messages with a staging exception linked to the CSV type.
      await replayEligibleStagingExceptions(context, replayData)

      // Attempt to replay messages with a timeseries staging exception linked to the CSV type.
      await replayEligibleTimeseriesStagingExceptions(context, replayData)
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

async function getCsvData (context, refreshData) {
  let configAuthorization = process.env['CONFIG_AUTHORIZATION']

  const refreshDataConfig = {
    method: 'get',
    headers: configAuthorization ? { Authorization: 'token ' + configAuthorization } : {}
  }

  configAuthorization = ''
  refreshData.csvResponse = await fetch(refreshData.csvUrl, refreshDataConfig)
  if (refreshData.csvResponse.status === 200 && refreshData.csvResponse.url.includes('.csv')) {
    return refreshData
  } else {
    throw new Error(`No csv file detected`)
  }
}

async function buildPreparedStatementParameters (context, row, refreshData) {
  const preparedStatementExecuteObject = {}
  // check all the expected values are present in the csv row and exclude incomplete csvRows.
  for (const columnObject of refreshData.functionSpecificData) {
    let columnName = columnObject.tableColumnName
    let expectedCsvKey = columnObject.expectedCSVKey

    if (row[expectedCsvKey] || columnObject.nullValueOverride === true) {
      if (columnObject.preprocessor) {
        preparedStatementExecuteObject[columnName] = columnObject.preprocessor(row[expectedCsvKey], columnName)
      } else {
        preparedStatementExecuteObject[columnName] = row[expectedCsvKey]
      }
    } else {
      return { rowError: true }
    }
  }
  return preparedStatementExecuteObject
}

async function processCsvRow (context, preparedStatement, row, refreshData) {
  try {
    let result = await buildPreparedStatementParameters(context, row, refreshData)
    if (result.rowError) {
      context.log.warn(`row is missing data.`)
      const failedRowInfo = {
        rowData: row,
        errorMessage: `row is missing data.`,
        errorCode: `NA`
      }
      refreshData.failedCsvRows.push(failedRowInfo)
    } else {
      await preparedStatement.execute(result)
    }
  } catch (err) {
    context.log.warn(`An error has been found in a row.\nError : ${err}.`)
    const failedRowInfo = {
      rowData: row,
      errorMessage: err.message,
      errorCode: err.code
    }
    refreshData.failedCsvRows.push(failedRowInfo)
  }
}

async function processCsvRows (context, transaction, preparedStatement, refreshData) {
  await new sql.Request(transaction).query(refreshData.deleteStatement)
  for (const columnObject of refreshData.functionSpecificData) {
    if (columnObject.tableColumnType === 'Decimal') {
      await preparedStatement.input(columnObject.tableColumnName, sql.Decimal(columnObject.precision, columnObject.scale))
    } else {
      await preparedStatement.input(columnObject.tableColumnName, sql[`${columnObject.tableColumnType}`])
    }
  }
  await preparedStatement.prepare(refreshData.insertPreparedStatement)
  for (const row of refreshData.csvRows) {
    await processCsvRow(context, preparedStatement, row, refreshData)
  }
  // Future requests will fail until the prepared statement is unprepared.
  await preparedStatement.unprepare()
}

async function refreshInternal (context, preparedStatement, refreshData) {
  try {
    const transaction = preparedStatement.parent
    refreshData.failedCsvRows = []
    await getCsvData(context, refreshData)
    refreshData.csvRows = await neatCsv(refreshData.csvResponse.body)
    // Do not refresh the table if the csv is empty.
    if (refreshData.csvRows.length > 0) {
      await processCsvRows(context, transaction, preparedStatement, refreshData)
    } else {
      context.log.warn(`No records detected - Aborting ${refreshData.csvSourceFile} refresh.`)
    }

    let csvLoadResult = await new sql.Request(transaction).query(refreshData.countStatement)
    context.log.info(`The ${refreshData.csvSourceFile} table now contains ${csvLoadResult.recordset[0].number} new/updated records`)
    if (csvLoadResult.recordset[0].number === 0) {
      // If all the records in the csv were invalid, this query needs rolling back to avoid a blank database overwrite.
      await transaction.rollback()
      context.log.warn('There were 0 new records to insert, a null database overwrite is not allowed. Transaction rolled back.')
    }
    // Regardless of whether a rollback took place (e.g in the case of zero rows passing verification), all the failed csv rows are captured for loading into exceptions.
    context.log.warn(`The ${refreshData.csvSourceFile} csv loader failed to load ${refreshData.failedCsvRows.length} csvRows.`)
  } catch (err) {
    context.log.error(`Refresh ${refreshData.csvSourceFile} data failed: ${err}`)
    throw err
  }
}
