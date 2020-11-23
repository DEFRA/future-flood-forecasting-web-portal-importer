const commonCoastalLocationRefreshData = require('../Shared/csv-load-handler/coastal-location-configuration')
const refresh = require('../Shared/csv-load-handler/shared-refresh-csv-rows')

const functionSpecificData = [
  { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' },
  { tableColumnName: 'TA_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'TAName' }
]

module.exports = async function (context) {
  const localRefreshData = {
    csvUrl: process.env.COASTAL_TRITON_FORECAST_LOCATION_URL,
    tableName: 'coastal_forecast_location',
    csvSourceFile: 'triton coastal location',
    deleteStatement: 'delete from fff_staging.coastal_forecast_location where coastal_type = \'triton\'',
    countStatement: 'select count(*) as number from fff_staging.coastal_forecast_location where coastal_type = \'triton\'',
    insertPreparedStatement: `
      insert into 
        fff_staging.coastal_forecast_location (fffs_loc_id, coastal_order, centre, mfdo_area, ta_name, coastal_type)
      values 
        (@fffs_loc_id, @coastal_order, @centre, @mfdo_area, @ta_name, @coastal_type)`
  }

  const refreshData = Object.assign(localRefreshData, commonCoastalLocationRefreshData.commonRefreshData)
  refreshData.functionSpecificData = functionSpecificData.concat(commonCoastalLocationRefreshData.commonFunctionSpecificData)
  await refresh(context, refreshData)
}
