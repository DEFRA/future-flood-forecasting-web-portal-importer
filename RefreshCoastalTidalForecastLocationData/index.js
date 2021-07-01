const commonRefreshData = require('../Shared/csv-load/common-refresh-data')
const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')

module.exports = async function (context) {
  const localRefreshData = {
    csvUrl: process.env.COASTAL_TIDAL_FORECAST_LOCATION_URL,
    csvSourceFile: 'tidal coastal location',
    deleteStatement: 'delete from fff_staging.coastal_forecast_location where coastal_type = \'Coastal Forecasting\'',
    countStatement: 'select count(*) as number from fff_staging.coastal_forecast_location where coastal_type = \'Coastal Forecasting\'',
    insertPreparedStatement: `
      insert into 
        fff_staging.coastal_forecast_location (fffs_loc_id, fffs_loc_name, coastal_order, centre, coastal_type, location_x, location_y, location_z, ta_name, mfdo_area)
      values 
        (@fffs_loc_id, @fffs_loc_name, @coastal_order, @centre, @coastal_type, @location_x, @location_y, @location_z, @ta_name, @mfdo_area)`,
    functionSpecificData: [
      { tableColumnName: 'LOCATION_Z', tableColumnType: 'Decimal', expectedCSVKey: 'LocationZ', precision: 38, scale: 8, nullValueOverride: true, preprocessor: commonRefreshData.returnNullForNaN },
      { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea', nullValueOverride: true },
      { tableColumnName: 'TA_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'TAName', nullValueOverride: true }
    ]
  }
  const refreshData = Object.assign(localRefreshData, commonRefreshData.commonCoastalLocationRefreshData)
  refreshData.functionSpecificData.push(...commonRefreshData.commonCoastalLocationFunctionSpecificData)
  await refresh(context, refreshData)
}
