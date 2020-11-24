const commonCoastalLocationRefreshData = require('../Shared/csv-load-handler/common-refresh-data')
const refresh = require('../Shared/csv-load-handler/shared-refresh-csv-rows')

module.exports = async function (context) {
  const localRefreshData = {
    csvUrl: process.env.COASTAL_TIDAL_FORECAST_LOCATION_URL,
    csvSourceFile: 'tidal coastal location',
    deleteStatement: 'delete from fff_staging.coastal_forecast_location where coastal_type = \'Coastal Forecasting\'',
    countStatement: 'select count(*) as number from fff_staging.coastal_forecast_location where coastal_type = \'Coastal Forecasting\'',
    insertPreparedStatement: `
      insert into 
        fff_staging.coastal_forecast_location (fffs_loc_id, fffs_loc_name, coastal_order, centre, coastal_type)
      values 
        (@fffs_loc_id, @fffs_loc_name, @coastal_order, @centre, @coastal_type)`,
    functionSpecificData: [
      { tableColumnName: 'FFFS_LOC_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName' }
    ]
  }
  const refreshData = Object.assign(localRefreshData, commonCoastalLocationRefreshData.commonRefreshData)
  refreshData.functionSpecificData.push(...commonCoastalLocationRefreshData.commonCoastalLocationFunctionSpecificData)
  await refresh(context, refreshData)
}
