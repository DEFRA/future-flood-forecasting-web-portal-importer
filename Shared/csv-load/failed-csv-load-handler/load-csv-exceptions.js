const createCSVStagingException = require('./create-csv-staging-exception')

module.exports = async function (transaction, context, sourceFile, failedRows) {
  for (let i = 0; i < failedRows.length; i++) {
    const csvStagingExceptionData = {
      sourceFile,
      rowData: failedRows[i].rowData,
      description: failedRows[i].errorMessage
    }
    try {
      await createCSVStagingException(context, transaction, csvStagingExceptionData)
    } catch (err) {
      context.log.warn(`Error loading row: ${failedRows[i].rowData} into csv-staging-exception.`)
    }
  }
}
