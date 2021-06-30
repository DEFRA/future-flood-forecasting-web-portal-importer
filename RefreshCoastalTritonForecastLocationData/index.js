const commonRefreshData = require('../Shared/csv-load/common-refresh-data')
const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')

module.exports = async function (context) {
  const localRefreshData = {
    csvUrl: process.env.COASTAL_TRITON_FORECAST_LOCATION_URL,
    tableName: 'coastal_forecast_location',
    csvSourceFile: 'triton coastal location',
    deleteStatement: 'delete from fff_staging.coastal_forecast_location where coastal_type = \'triton\'',
    countStatement: 'select count(*) as number from fff_staging.coastal_forecast_location where coastal_type = \'triton\'',
    insertPreparedStatement: `
      insert into 
        fff_staging.coastal_forecast_location (fffs_loc_id, coastal_order, centre, mfdo_area, ta_name, coastal_type, fffs_loc_name, location_x, location_y)
      values 
        (@fffs_loc_id, @coastal_order, @centre, @mfdo_area, @ta_name, @coastal_type, @fffs_loc_name, @location_x, @location_y)`,
    functionSpecificData: [
      { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' },
      { tableColumnName: 'TA_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'TAName' },
      { tableColumnName: 'FFFS_LOC_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName', nullValueOverride: true }
    ]
  }

  const refreshData = Object.assign(localRefreshData, commonRefreshData.commonCoastalLocationRefreshData)
  refreshData.functionSpecificData.push(...commonRefreshData.commonCoastalLocationFunctionSpecificData)
  await refresh(context, refreshData)
}
