const commonCoastalLocationRefreshData = require('../Shared/csv-load-handler/coastal-location-configuration')
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
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'FFFS_LOC_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
      { tableColumnName: 'FFFS_LOC_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName' },
      { tableColumnName: 'COASTAL_ORDER', tableColumnType: 'Int', expectedCSVKey: 'CoastalOrder' },
      { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
      { tableColumnName: 'COASTAL_TYPE', tableColumnType: 'NVarChar', expectedCSVKey: 'Type' }
    ]
  }
  const refreshData = Object.assign(localRefreshData, commonCoastalLocationRefreshData)
  await refresh(context, refreshData)
}
