const { doInTransaction, executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const createCSVStagingException = require('../Shared/failed-csv-load-handler/create-csv-staging-exception')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, message) {
  async function refresh (transaction, context) {
    await executePreparedStatementInTransaction(refreshForecastLocationData, context, transaction)
  }

  // Refresh the data in the forecast location table within a transaction with a serializable isolation
  // level so that refresh is prevented if the forecast location table is in use. If the forecast location
  // table is in use and forecast location table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, 'The fluvial_forecast_location refresh has failed with the following error:', sql.ISOLATION_LEVEL.SERIALIZABLE)
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}

async function refreshForecastLocationData (context, preparedStatement) {
  try {
    const transaction = preparedStatement.parent
    const response = await fetch(`${process.env['FLUVIAL_FORECAST_LOCATION_URL']}`)
    const rows = await neatCsv(response.body)
    const recordCountResponse = rows.length

    // Do not refresh the forecast location table if the csv is empty.
    if (recordCountResponse > 0) {
      await new sql.Request(transaction).batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FLUVIAL_FORECAST_LOCATION`)

      const failedRows = []
      await preparedStatement.input('CENTRE', sql.NVarChar)
      await preparedStatement.input('MFDO_AREA', sql.NVarChar)
      await preparedStatement.input('CATCHMENT', sql.NVarChar)
      await preparedStatement.input('FFFS_LOCATION_ID', sql.NVarChar)
      await preparedStatement.input('FFFS_LOCATION_NAME', sql.NVarChar)
      await preparedStatement.input('PLOT_ID', sql.NVarChar)
      await preparedStatement.input('DRN_ORDER', sql.Int)
      await preparedStatement.input('DISPLAY_ORDER', sql.Int)
      await preparedStatement.input('DATUM', sql.NVarChar)

      await preparedStatement.prepare(`INSERT INTO ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FLUVIAL_FORECAST_LOCATION (CENTRE, MFDO_AREA, CATCHMENT, FFFS_LOCATION_ID, FFFS_LOCATION_NAME, PLOT_ID, DRN_ORDER, DISPLAY_ORDER, DATUM) values (@CENTRE, @MFDO_AREA, @CATCHMENT, @FFFS_LOCATION_ID, @FFFS_LOCATION_NAME, @PLOT_ID, @DRN_ORDER, @DISPLAY_ORDER, @DATUM)`)
      for (const row of rows) {
        // Ignore rows in the CSV data that do not have entries for all columns.
        try {
          if (row.Centre && row.MFDOArea && row.Catchment && row.FFFSLocID && row.FFFSLocName && row.PlotID && row.DRNOrder && row.Order && row.Datum) {
            await preparedStatement.execute({
              CENTRE: row.Centre,
              MFDO_AREA: row.MFDOArea,
              CATCHMENT: row.Catchment,
              FFFS_LOCATION_ID: row.FFFSLocID,
              FFFS_LOCATION_NAME: row.FFFSLocName,
              PLOT_ID: row.PlotID,
              DRN_ORDER: row.DRNOrder,
              DISPLAY_ORDER: row.Order,
              DATUM: row.Datum
            })
          } else {
            const failedRowInfo = {
              rowData: row,
              errorMessage: `A row is missing data.`,
              errorCode: `NA`
            }
            failedRows.push(failedRowInfo)
          }
        } catch (err) {
          context.log.warn(`an error has been found in a row with the Location ID: ${row.FFFSLocID}.\n  Error : ${err}`)
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
          `Forecast location`, // args - csv file
          failedRows[i].rowData, // args - row data
          failedRows[i].errorMessage // args - error description
        )
      }
      context.log.error(`The forecast location csv loader has ${failedRows.length} failed row inserts.`)
    } else {
      // If the csv is empty then the file is essentially ignored
      context.log.warn('No records detected - Aborting fluvial_forecast_location refresh')
    }
    const result = await new sql.Request(transaction).query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FLUVIAL_FORECAST_LOCATION`)
    context.log.info(`The fluvial_forecast_location table contains ${result.recordset[0].number} records`)
    if (result.recordset[0].number === 0) {
      // If all the records in the csv were invalid, the function will overwrite records in the table with no new records
      // after the table has already been truncated. This function needs rolling back to avoid a blank database overwrite.
      context.log.warn('There are no new records to insert, rolling back fluvial_forecast_location refresh')
      throw new Error('A null database overwrite is not allowed')
    }
  } catch (err) {
    context.log.error(`Refresh forecast location data failed: ${err}`)
    throw err
  }
}
