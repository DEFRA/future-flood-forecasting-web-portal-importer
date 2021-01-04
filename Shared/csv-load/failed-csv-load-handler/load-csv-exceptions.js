const createCSVStagingException = require('./create-csv-staging-exception')

module.exports = async function (transaction, context, sourceFile, failedRows) {
  for (let i = 0; i < failedRows.length; i++) {
    try {
      await createCSVStagingException(context, transaction, sourceFile, failedRows[i].rowData, failedRows[i].errorMessage)
    } catch (err) {
      context.log.warn(`Error loading row: ${failedRows[i].rowData} into csv-staging-exception.`)
    }
  }
}
