const { doInTransaction } = require('../../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = function (context) {
  this.insertCSVStagingException = async function () {
    await doInTransaction(insertCSVStagingException, context, 'Unable to insert csv staging exception data', null)
  }
  this.checkReplayedStagingExceptionMessages = async function (expectedReplayedStagingExceptionMessages) {
    await doInTransaction(checkReplayedStagingExceptionMessages, context, 'Unable to check replayed staging exception data', null, expectedReplayedStagingExceptionMessages)
  }
}

async function insertCSVStagingException (transaction, context) {
  await new sql.Request(transaction).batch(`
  insert into 
    fff_staging.csv_staging_exception (csv_source_file, row_data, description, exception_time, workflow_id)
  values  
    ('other', 'data', 'test data', getutcdate(), 'workflow')`
  )
}

async function checkReplayedStagingExceptionMessages (transaction, context, expectedStagingExceptionMessages) {
  for (const stagingExceptionMessage of context.bindings.processFewsEventCode) {
    expect(expectedStagingExceptionMessages).toContainEqual(stagingExceptionMessage)
  }
}
