const { doInTransaction } = require('../Shared/transaction-helper')
const fetch = require('node-fetch')
const neatCsv = require('neat-csv')
const sql = require('mssql')

module.exports = async function (context, message) {
  async function refresh (transactionData) {
    await refreshForecastLocationData(transactionData.preparedStatement, transactionData.transaction, context)
  }

  // Refresh the data in the forecast location table within a transaction with a serializable isolation
  // level so that refresh is prevented if the forecast location table is in use. If the forecast location
  // table is in use and forecast location table lock acquisition fails, the function invocation will fail.
  // In most cases function invocation will be retried automatically and should succeed.  In rare
  // cases where successive retries fail, the message that triggers the function invocation will be
  // placed on a dead letter queue.  In this case, manual intervention will be required.
  await doInTransaction(refresh, context, sql.ISOLATION_LEVEL.SERIALIZABLE)

  sql.on('error', err => {
    context.log.error(err)
    throw err
  })
  // context.done() not requried as the async function returns the desired result, there is no output binding to be activated.
}

async function refreshForecastLocationData (preparedStatement, transaction, context) {
  const response = await fetch(`${process.env['FORECAST_LOCATION_URL']}`)
  let rows = await neatCsv(response.body)
  const recordCountResponse = rows.length

  // Do not refresh the forecast location table if the csv is empty.
  if (recordCountResponse > 0) {
    let request = new sql.Request(transaction)
    await request.batch(`delete from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION`)

    await preparedStatement.input('CENTRE', sql.NVarChar)
    await preparedStatement.input('MFDO_AREA', sql.NVarChar)
    await preparedStatement.input('CATCHMENT', sql.NVarChar)
    await preparedStatement.input('FFFS_LOCATION_ID', sql.NVarChar)
    await preparedStatement.input('FFFS_LOCATION_NAME', sql.NVarChar)
    await preparedStatement.input('PLOT_ID', sql.NVarChar)
    await preparedStatement.input('DRN_ORDER', sql.Int)
    await preparedStatement.prepare(`INSERT INTO ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION (CENTRE, MFDO_AREA, CATCHMENT, FFFS_LOCATION_ID, FFFS_LOCATION_NAME, PLOT_ID, DRN_ORDER) values (@CENTRE, @MFDO_AREA, @CATCHMENT, @FFFS_LOCATION_ID, @FFFS_LOCATION_NAME, @PLOT_ID, @DRN_ORDER)`)
    for (const row of rows) {
      // Ignore rows in the CSV data that do not have entries for all columns.
      try {
        if (row.Centre && row.MFDOArea && row.Catchment && row.FFFSLocID && row.FFFSLocName && row.PlotID && row.DRNOrder) {
          await preparedStatement.execute({
            CENTRE: row.Centre,
            MFDO_AREA: row.MFDOArea,
            CATCHMENT: row.Catchment,
            FFFS_LOCATION_ID: row.FFFSLocID,
            FFFS_LOCATION_NAME: row.FFFSLocName,
            PLOT_ID: row.PlotID,
            DRN_ORDER: row.DRNOrder
          })
        }
      } catch (err) {
        context.log.warn(`an error has been found in a row with the Location ID: ${row.FFFSLocID}.\n  Error : ${err}`)
      }
    }
    // Future requests will fail until the prepared statement is unprepared.
    await preparedStatement.unprepare()
  } else {
    context.log.warn('no records detected - Aborting forecast_location refresh')
  }
  let request = new sql.Request(transaction)
  const result = await request.query(`select count(*) as number from ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.FORECAST_LOCATION`)
  context.log.info(`The forecast_location table contains ${result.recordset[0].number} records`)
}
