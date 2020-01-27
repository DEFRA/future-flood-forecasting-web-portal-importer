const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const createCSVStagingException = require('../Shared/create-csv-staging-exception')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, message) {
  async function refresh (transaction, context) {
    await executePreparedStatementInTransaction(refreshIgnoredWorkflowData, context, transaction)
  }

  // Refresh with a serializable isolation level so that refresh is prevented if the ignored_workflow table is in use.
  // If the table is in use and table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The ignored workflow refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}

async function refreshIgnoredWorkflowData (context, preparedStatement) {
  try {
    const transaction = preparedStatement.parent
    const response = await fetch(`${process.env['IGNORED_WORKFLOW_URL']}`)
    const rows = await neatCsv(response.body)
    const recordCountResponse = rows.length

    // Do not refresh the ignored workflow table if the csv is empty.
    if (recordCountResponse > 0) {
      await new sql.Request(transaction).batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow`)

      const failedRows = []
      await preparedStatement.input('WORKFLOW_ID', sql.NVarChar)
      await preparedStatement.prepare(`insert into ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.ignored_workflow (WORKFLOW_ID) values (@WORKFLOW_ID)`)
      for (const row of rows) {
        // Ignore rows in the CSV data that do not have entries for all columns.
        try {
          if (row.WorkflowID) {
            await preparedStatement.execute({
              WORKFLOW_ID: row.WorkflowID
            })
          } else {
            let failedRowInfo = {
              rowData: row,
              errorMessage: `A row is missing data.`,
              errorCode: `NA`
            }
            failedRows.push(failedRowInfo)
          }
        } catch (err) {
          context.log.warn(`an error has been found in a row with the WorkflowID: ${row.WorkflowID}.\n  Error : ${err}`)
          let failedRowInfo = {
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
