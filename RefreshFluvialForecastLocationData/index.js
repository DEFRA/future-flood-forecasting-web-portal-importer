
const refresh = require('../Shared/shared-refresh-csv-rows')

module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env.FLUVIAL_FORECAST_LOCATION_URL,
    tableName: 'fluvial_forecast_location',
    csvSourceFile: 'fluvial forecast location refresh',
    deleteStatement: 'delete from fff_staging.fluvial_forecast_location',
    countStatement: 'select count(*) as number from fff_staging.fluvial_forecast_location',
    insertPreparedStatement: `
      insert into 
        fff_staging.fluvial_forecast_location (fffs_location_id, fffs_location_name, drn_order, datum, display_order, centre, plot_id, catchment, catchment_order, mfdo_area)       
      values 
        (@fffs_location_id, @fffs_location_name, @drn_order, @datum, @display_order, @centre, @plot_id, @catchment, @catchment_order, @mfdo_area)`,
    // Column information and corresponding csv information
    functionSpecificData: [
      { tableColumnName: 'FFFS_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
      { tableColumnName: 'FFFS_LOCATION_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName' },
      { tableColumnName: 'DRN_ORDER', tableColumnType: 'Int', expectedCSVKey: 'DRNOrder' },
      { tableColumnName: 'DATUM', tableColumnType: 'NVarChar', expectedCSVKey: 'Datum', nullValueOverride: true },
      { tableColumnName: 'DISPLAY_ORDER', tableColumnType: 'Int', expectedCSVKey: 'Order' },
      { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
      { tableColumnName: 'PLOT_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'PlotID' },
      { tableColumnName: 'CATCHMENT', tableColumnType: 'NVarChar', expectedCSVKey: 'Catchment' },
      { tableColumnName: 'CATCHMENT_ORDER', tableColumnType: 'Int', expectedCSVKey: 'CatchmentOrder' },
      { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' }
    ]
  }
  await refresh(context, refreshData)
}
