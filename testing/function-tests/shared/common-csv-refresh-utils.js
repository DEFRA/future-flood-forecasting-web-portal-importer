const { doInTransaction } = require('../../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = function (context) {
  this.insertCSVStagingException = async function () {
    await doInTransaction(insertCSVStagingException, context, 'Unable to insert csv staging exception data', null)
  }
  this.checkReplayedStagingExceptionMessages = async function (expectedReplayedStagingExceptionMessages) {
    expect(context.bindings.processFewsEventCode.length).toBe((expectedReplayedStagingExceptionMessages || []).length)
    for (const stagingExceptionMessage of expectedReplayedStagingExceptionMessages || []) {
      expect(context.bindings.processFewsEventCode).toContainEqual(stagingExceptionMessage)
    }
  }
  this.checkReplayedTimeseriesStagingExceptionMessages = async function (expectedReplayedTimeseriesStagingExceptionMessages) {
    expect(context.bindings.importFromFews.length).toBe((expectedReplayedTimeseriesStagingExceptionMessages || []).length)
    for (const timeseriesStagingExceptionMessage of expectedReplayedTimeseriesStagingExceptionMessages || []) {
      expect(context.bindings.importFromFews).toContainEqual(timeseriesStagingExceptionMessage)
    }
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
