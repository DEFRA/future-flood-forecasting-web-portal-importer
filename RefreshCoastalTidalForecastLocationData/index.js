const commonCoastalLocationRefreshData = require('../Shared/csv-load-handler/coastal-location-configuration')
const refresh = require('../Shared/csv-load-handler/shared-refresh-csv-rows')

// Column information and corresponding csv information
const functionSpecificData = [
  { tableColumnName: 'FFFS_LOC_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName' }
]

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
        (@fffs_loc_id, @fffs_loc_name, @coastal_order, @centre, @coastal_type)`
  }
  const refreshData = Object.assign(localRefreshData, commonCoastalLocationRefreshData.commonRefreshData)
  refreshData.functionSpecificData = functionSpecificData.concat(commonCoastalLocationRefreshData.commonFunctionSpecificData)
  await refresh(context, refreshData)
}
