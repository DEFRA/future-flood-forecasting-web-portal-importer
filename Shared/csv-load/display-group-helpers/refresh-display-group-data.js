const transferAggregatedRecords = require('./transfer-aggregated-records')
const countTableRecords = require('../../../Shared/count-table-records')

module.exports = async function refreshDisplayGroupTable (transaction, context, tempTableName, tableName) {
  try {
    let tempRecordCount = await countTableRecords(context, transaction, tempTableName)
    // Do not refresh the display_group_workflow table if the local temporary table is empty.
    if (tempRecordCount > 0) {
      await transferAggregatedRecords(context, transaction, tempTableName, tableName)
    } else {
      // If the csv is empty then the file is essentially ignored
      context.log.warn(`${tempTableName} contains no records - Aborting ${tableName} refresh`)
    }

    let recordCount = await countTableRecords(context, transaction, `fff_staging.${tableName}`)
    if (recordCount === 0) {
      // If all the records in the csv (inserted into the temp table) are invalid, the function will overwrite records in the table with no new records
      // after the table has already been truncated. This function needs rolling back to avoid a blank database overwrite.
      // # The temporary table protects this from happening greatly reducing the likelihood of occurance.
      context.log.warn(`There are no new records to insert into ${tableName}, rolling back refresh`)
      throw new Error('A null database overwrite is not allowed')
    }
  } catch (err) {
    context.log.error(`Refresh ${tableName} data failed: ${err}`)
    throw err
  }
}
