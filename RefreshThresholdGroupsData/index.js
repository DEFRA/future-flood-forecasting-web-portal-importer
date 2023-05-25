const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')

const functionSpecificData = [
  { tableColumnName: 'GROUP_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'groupID' },
  { tableColumnName: 'GROUP_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'groupName' },
  { tableColumnName: 'THRESHOLD_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'thresholdID' },
  { tableColumnName: 'THRESHOLD_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'thresholdName' },
  { tableColumnName: 'SHORT_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'shortName' }
]

module.exports = async function (context) {
  // Ideally, these constants would be replaced with inline attributes within refreshData.
  // Inline attributes caused code climate to report code duplication. Using a number of
  // distinct constants satisfies code climate.
  const tableName = 'threshold_groups'
  const nonWorkflowRefreshCsvType = 'TGR'
  const csvUrl = process.env.THRESHOLD_GROUPS_URL
  const csvSourceFile = 'threshold groups refresh'

  const refreshData = {
    deleteStatement: 'delete from fff_staging.threshold_groups',
    countStatement: 'select count(*) as number from fff_staging.threshold_groups',
    insertPreparedStatement: `
      insert into 
        fff_staging.threshold_groups (group_id, group_name, threshold_id, threshold_name, short_name)
      values 
        (@group_id, @group_name, @threshold_id, @threshold_name, @short_name)`,
    tableName,
    nonWorkflowRefreshCsvType,
    csvUrl,
    csvSourceFile,
    // Column information and corresponding csv information
    functionSpecificData
  }

  await refresh(context, refreshData)
}
