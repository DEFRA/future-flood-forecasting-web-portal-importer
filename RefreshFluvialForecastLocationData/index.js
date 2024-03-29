const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')
const commonRefreshData = require('../Shared/csv-load/common-refresh-data')

const functionSpecificData = [
  { tableColumnName: 'FFFS_LOCATION_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
  { tableColumnName: 'FFFS_LOCATION_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName' },
  { tableColumnName: 'RIVER_LOCAL', tableColumnType: 'NVarChar', expectedCSVKey: 'RiverLocal', nullValueOverride: true },
  { tableColumnName: 'DRN_ORDER', tableColumnType: 'Int', expectedCSVKey: 'DRNOrder' },
  { tableColumnName: 'DATUM', tableColumnType: 'NVarChar', expectedCSVKey: 'Datum', nullValueOverride: true },
  { tableColumnName: 'DISPLAY_ORDER', tableColumnType: 'Int', expectedCSVKey: 'Order' },
  { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
  { tableColumnName: 'PLOT_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'PlotID' },
  { tableColumnName: 'CATCHMENT', tableColumnType: 'NVarChar', expectedCSVKey: 'Catchment' },
  { tableColumnName: 'CATCHMENT_ORDER', tableColumnType: 'Int', expectedCSVKey: 'CatchmentOrder' },
  { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' },
  { tableColumnName: 'LOCATION_X', tableColumnType: 'Decimal', expectedCSVKey: 'LocationX', precision: 38, scale: 8, nullValueOverride: true, preprocessor: commonRefreshData.returnNullForNaN },
  { tableColumnName: 'LOCATION_Y', tableColumnType: 'Decimal', expectedCSVKey: 'LocationY', precision: 38, scale: 8, nullValueOverride: true, preprocessor: commonRefreshData.returnNullForNaN },
  { tableColumnName: 'LOCATION_Z', tableColumnType: 'Decimal', expectedCSVKey: 'LocationZ', precision: 38, scale: 8, nullValueOverride: true, preprocessor: commonRefreshData.returnNullForNaN }
]
module.exports = async function (context) {
  const refreshData = {
    csvUrl: process.env.FLUVIAL_FORECAST_LOCATION_URL,
    nonWorkflowRefreshCsvType: 'FFL',
    tableName: 'fluvial_forecast_location',
    csvSourceFile: 'fluvial forecast location refresh',
    deleteStatement: 'delete from fff_staging.fluvial_forecast_location',
    countStatement: 'select count(*) as number from fff_staging.fluvial_forecast_location',
    insertPreparedStatement: `
      insert into 
        fff_staging.fluvial_forecast_location (fffs_location_id, fffs_location_name, river_local, drn_order, datum, display_order, centre, plot_id, catchment, catchment_order, mfdo_area, location_x, location_y, location_z)
      values 
        (@fffs_location_id, @fffs_location_name, @river_local, @drn_order, @datum, @display_order, @centre, @plot_id, @catchment, @catchment_order, @mfdo_area, @location_x, @location_y, @location_z)`,
    // Column information and corresponding csv information
    functionSpecificData
  }
  await refresh(context, refreshData)
}
