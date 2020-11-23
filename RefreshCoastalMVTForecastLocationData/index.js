const commonCoastalLocationRefreshData = require('../Shared/csv-load-handler/coastal-location-configuration')
const refresh = require('../Shared/csv-load-handler/shared-refresh-csv-rows')

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
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'FFFS_LOC_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
      { tableColumnName: 'COASTAL_ORDER', tableColumnType: 'Int', expectedCSVKey: 'CoastalOrder' },
      { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
      { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' },
      { tableColumnName: 'TA_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'TAName' },
      { tableColumnName: 'COASTAL_TYPE', tableColumnType: 'NVarChar', expectedCSVKey: 'Type' }
    ]
  }
  const refreshData = Object.assign(localRefreshData, commonCoastalLocationRefreshData)
  await refresh(context, refreshData)
}
