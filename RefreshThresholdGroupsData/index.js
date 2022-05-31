
const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')

const functionSpecificData = [
  { tableColumnName: 'GROUP_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'groupID' },
  { tableColumnName: 'GROUP_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'groupName' },
  { tableColumnName: 'THRESHOLD_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'thresholdID' },
  { tableColumnName: 'THRESHOLD_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'thresholdName' },
  { tableColumnName: 'SHORT_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'shortName' }
]

module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env.THRESHOLD_GROUPS_URL,
    nonWorkflowRefreshCsvType: 'TGR',
    tableName: 'threshold_groups',
    csvSourceFile: 'threshold groups refresh',
    deleteStatement: 'delete from fff_staging.threshold_groups',
    countStatement: 'select count(*) as number from fff_staging.threshold_groups',
    insertPreparedStatement: `
      insert into 
        fff_staging.threshold_groups (group_id, group_name, threshold_id, threshold_name, short_name)
      values 
        (@group_id, @group_name, @threshold_id, @threshold_name, @short_name)`,
    // Column information and corresponding csv information
    functionSpecificData
  }

  await refresh(context, refreshData)
}
