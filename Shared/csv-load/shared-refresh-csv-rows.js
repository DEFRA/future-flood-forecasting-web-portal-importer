const replayEligibleTimeseriesStagingExceptions = require('../message-replay/replay-eligible-timeseries-staging-exceptions')
const deleteCsvStagingExceptions = require('./failed-csv-load-handler/delete-csv-staging-exception')
const replayEligibleStagingExceptions = require('../message-replay/replay-eligible-staging-exceptions')
const { doInTransaction, executePreparedStatementInTransaction } = require('../transaction-helper')
const loadExceptions = require('./failed-csv-load-handler/load-csv-exceptions')
const { processServiceConfigurationUpdateForAllCsvDataIfNeeded } = require('./service-configuration-update-utils')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, refreshData) {
  const isolationLevel = sql.ISOLATION_LEVEL.SERIALIZABLE

  // Transaction 1 - Refresh CSV data
  // Refresh with a serializable isolation level so that refresh is prevented if the table is in use.
  // If the table is in use and table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction({ fn: refreshInTransaction, context, errorMessage: `The ${refreshData.csvSourceFile} refresh has failed with the following error:`, isolationLevel }, refreshData)

  // Transaction 2
  // If a rollback has not occurred record the time of refresh and replay messages if needed.
  if (refreshData.refreshRollbackRequested === false) {
    await doInTransaction({ fn: recordCsvRefreshTimeAndReplayMessagesIfNeeded, context, errorMessage: 'Service configuration update detection after CSV refresh failed with the following error:', isolationLevel }, refreshData)
  }

  // Transaction 3
  // Regardless of rollback, all exceptions should be recorded.
  if (refreshData.failedCsvRows.length > 0) {
    await doInTransaction({ fn: loadExceptions, context, errorMessage: `The ${refreshData.csvSourceFile} exception load has failed with the following error:`, isolationLevel }, refreshData.csvSourceFile, refreshData.failedCsvRows)
  } else {
    context.log.info('There were no csv exceptions during load.')
  }

  // Transaction 4
  // Send a notification if a full service configuration update has been detected.
  await doInTransaction({ fn: performServiceConfigurationUpdateCheckForAllCsvData, context, errorMessage: 'Failed to check for full service configuration update and send notification if needed', isolationLevel })
  // context.done() not required as the async function returns the desired result, there is no output binding to be activated.
}

async function recordCsvRefreshTimeAndReplayMessagesIfNeeded (transaction, context, refreshData) {
  await executePreparedStatementInTransaction(updateCsvRefreshTimeTable, context, transaction, refreshData)

  const replayData = {
    csvType: refreshData.workflowRefreshCsvType || refreshData.nonWorkflowRefreshCsvType,
    transaction
  }

  if (refreshData.workflowRefreshCsvType) {
    // Attempt to replay messages with a staging exception linked to the CSV type.
    await replayEligibleStagingExceptions(context, replayData)

    // Attempt to replay messages with a timeseries staging exception linked to the CSV type.
    await replayEligibleTimeseriesStagingExceptions(context, replayData)
  }
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
    refreshData.refreshRollbackRequested = false
    if (refreshData.postOperation) {
      await refreshData.postOperation(transaction, context)
    }
    // remove the outdated csv staging exceptions for this csv csvSourceFile
    await executePreparedStatementInTransaction(deleteCsvStagingExceptions, context, transaction, refreshData.csvSourceFile)
  }
}

async function updateCsvRefreshTimeTable (context, preparedStatement, refreshData) {
  const tableName = `${refreshData.nonWorkflowRefreshCsvType ? 'non_' : ''}workflow_refresh`
  await preparedStatement.input('csvType', sql.NVarChar)

  await preparedStatement.prepare(`
    merge fff_staging.${tableName} with (holdlock) as target
    using (values (@csvType, getutcdate())) as source (csv_type, refresh_time)
    on (target.csv_type = source.csv_type)
    when matched then
      update set target.refresh_time = source.refresh_time
    when not matched then
      insert (csv_type, refresh_time)
        values (csv_type, refresh_time);
  `)

  const parameters = {
    csvType: refreshData.workflowRefreshCsvType || refreshData.nonWorkflowRefreshCsvType
  }

  await preparedStatement.execute(parameters)
}

async function getCsvData (context, refreshData) {
  let configAuthorization = process.env.CONFIG_AUTHORIZATION

  const refreshDataConfig = {
    method: 'get',
    headers: configAuthorization ? { Authorization: 'token ' + configAuthorization } : {}
  }

  configAuthorization = ''
  refreshData.csvResponse = await fetch(refreshData.csvUrl, refreshDataConfig)
  if (refreshData.csvResponse.status === 200 && refreshData.csvResponse.url.includes('.csv')) {
    return refreshData
  } else {
    throw new Error('No csv file detected')
  }
}

async function buildPreparedStatementParameters (context, row, refreshData) {
  const preparedStatementExecuteObject = {}
  let returnValue = preparedStatementExecuteObject
  let rowError = false
  let index = 0
  const numberOfColumnObjects = refreshData.functionSpecificData.length
  // check all the expected values are present in the csv row and exclude incomplete csvRows.
  // Use a while loop with nested ternary operators to reduce the cognitive complexity rating.
  while (!rowError && index < numberOfColumnObjects) {
    const columnObject = refreshData.functionSpecificData[index]
    rowError = await buildPreparedStatementParametersForColumnIfPossible(context, row, preparedStatementExecuteObject, columnObject)
    rowError ? returnValue = { rowError: true } : index++
  }
  return returnValue
}

async function buildPreparedStatementParametersForColumnIfPossible (context, row, preparedStatementExecuteObject, columnObject) {
  const expectedCsvKey = columnObject.expectedCSVKey
  let rowError = false

  // The row must have a value for the current column or the current column must allow null values
  if (row[expectedCsvKey] || columnObject.nullValueOverride) {
    await buildPreparedStatementParametersForColumn(context, row, preparedStatementExecuteObject, columnObject)
  } else {
    rowError = true
  }

  return rowError
}

async function buildPreparedStatementParametersForColumn (context, row, preparedStatementExecuteObject, columnObject) {
  const columnName = columnObject.tableColumnName
  const expectedCsvKey = columnObject.expectedCSVKey

  let rowData = row[expectedCsvKey]
  if (columnObject.nullValueOverride === true && (row[expectedCsvKey] === null || row[expectedCsvKey] === '')) {
    rowData = null
  }
  if (columnObject.preprocessor) {
    preparedStatementExecuteObject[columnName] = columnObject.preprocessor(rowData, columnName)
  } else {
    preparedStatementExecuteObject[columnName] = rowData
  }
}

async function processCsvRow (context, preparedStatement, row, refreshData) {
  try {
    const rowExecuteObject = await buildPreparedStatementParameters(context, row, refreshData)
    if (rowExecuteObject.rowError) {
      context.log.warn('row is missing data.')
      const failedRowInfo = {
        rowData: row,
        errorMessage: 'row is missing data.',
        errorCode: 'NA'
      }
      refreshData.failedCsvRows.push(failedRowInfo)
    } else {
      await preparedStatement.execute(rowExecuteObject)
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
    // Clear the table in preparation for the refresh.
    await new sql.Request(transaction).query(refreshData.deleteStatement)
    refreshData.failedCsvRows = []
    await getCsvData(context, refreshData)
    refreshData.csvRows = await neatCsv(refreshData.csvResponse.body)
    // Do not refresh the table if the csv is empty.
    if (refreshData.csvRows.length > 0) {
      await processCsvRows(context, transaction, preparedStatement, refreshData)
    } else {
      context.log.warn(`No records detected - Aborting ${refreshData.csvSourceFile} refresh.`)
    }

    const csvLoadResult = await new sql.Request(transaction).query(refreshData.countStatement)
    context.log.info(`The ${refreshData.tableName} table now contains ${csvLoadResult.recordset[0].number} new/updated records`)
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

async function performServiceConfigurationUpdateCheckForAllCsvData (transaction, context) {
  await executePreparedStatementInTransaction(processServiceConfigurationUpdateForAllCsvDataIfNeeded, context, transaction)
}
