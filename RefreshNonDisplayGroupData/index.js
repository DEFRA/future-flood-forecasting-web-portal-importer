const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, message) {
  async function refresh (transaction, context) {
    await executePreparedStatementInTransaction(refreshNonDisplayGroupData, context, transaction)
  }
  // Refresh the data in the fluvial_non_display_group_workflow table within a transaction with a serializable isolation
  // level so that refresh is prevented if the fluvial_non_display_group_workflow table is in use. If the fluvial_non_display_group_workflow
  // table is in use and fluvial_non_display_group_workflow table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}

async function refreshNonDisplayGroupData (context, preparedStatement) {
  context.log.info('running')
  const transaction = preparedStatement.parent
  try {
    const response = await fetch(`${process.env['FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW_URL']}`)
    const rows = await neatCsv(response.body)
    const recordCountResponse = rows.length

    if (recordCountResponse > 0) {
      const request = new sql.Request(preparedStatement.parent)
      await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW`)

      await preparedStatement.input('WORKFLOW_ID', sql.NVarChar)
      await preparedStatement.input('FILTER_ID', sql.NVarChar)

      await preparedStatement.prepare(`
            INSERT INTO 
             ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW
                (WORKFLOW_ID, FILTER_ID)
            values
                (@WORKFLOW_ID, @FILTER_ID)`)
      for (const row of rows) {
        // Ignore rows in the CSV data that do not have entries for all columns.
        try {
          if (row.WorkflowID && row.FilterID) {
            // console.log(preparedStatement.statement)
            await preparedStatement.execute({
              WORKFLOW_ID: row.WorkflowID,
              FILTER_ID: row.FilterID
            })
          }
        } catch (err) {
          context.log.warn(`an error has been found in a row with the Workflow ID: ${row.WorkflowID}.\n  Error : ${err}`)
        }
      }
      // Future requests will fail until the prepared statement is unprepared.
      await preparedStatement.unprepare()
    } else {
      // If the csv is empty then the file is essentially ignored
      context.log.warn('No records detected - Aborting fluvial_non_display_group_workflows refresh')
    }
    const request = new sql.Request(transaction)
    const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW`)
    context.log.info(`The fluvial_non_display_group_workflow table now contains ${result.recordset[0].number} records`)
    if (result.recordset[0].number === 0) {
      // If all the records in the csv were invalid, the function will overwrite records in the table with no new records
      // after the table has already been truncated. This function needs rolling back to avoid a blank database overwrite.
      context.log.warn('There are no new records to insert, rolling back fluvial_non_display_group_workflow refresh.')
      context.log.info('A null database overwrite is not allowed, rolling back.')
      await transaction.rollback()
      context.log.info('Transaction rolled back.')
    }
  } catch (err) {
    context.log.error(`Refresh fluvial non display group workflow data failed: ${err}`)
    throw err
  }
}
