const commonRefreshData = require('../Shared/csv-load/common-refresh-data')
const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')

module.exports = async function (context) {
  const localRefreshData = {
    csvUrl: process.env.COASTAL_MVT_FORECAST_LOCATION_URL,
    csvSourceFile: 'mvt coastal location',
    deleteStatement: 'delete from fff_staging.coastal_forecast_location where coastal_type = \'Multivariate Thresholds\'',
    countStatement: 'select count(*) as number from fff_staging.coastal_forecast_location where coastal_type = \'Multivariate Thresholds\'',
    insertPreparedStatement: `
      insert into 
        fff_staging.coastal_forecast_location (fffs_loc_id, coastal_order, centre, mfdo_area, ta_name, coastal_type)
      values 
        (@fffs_loc_id, @coastal_order, @centre, @mfdo_area, @ta_name, @coastal_type)`,
    functionSpecificData: [
      { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' },
      { tableColumnName: 'TA_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'TAName' }
    ]
  }
  const refreshData = Object.assign(localRefreshData, commonRefreshData.commonCoastalLocationRefreshData)
  refreshData.functionSpecificData.push(...commonRefreshData.commonCoastalLocationFunctionSpecificData)
  await refresh(context, refreshData)
}
