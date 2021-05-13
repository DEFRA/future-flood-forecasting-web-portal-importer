import { commonRefreshData } from '../Shared/csv-load/common-refresh-data.js'
import refresh from '../Shared/csv-load/shared-refresh-csv-rows.js'

export default async function (context) {
  const localRefreshData = {
    csvUrl: process.env.COASTAL_MVT_FORECAST_LOCATION_URL,
    nonWorkflowRefreshCsvType: 'CMV',
    csvSourceFile: 'mvt coastal location',
    deleteStatement: 'delete from fff_staging.coastal_forecast_location where coastal_type = \'Multivariate Thresholds\'',
    countStatement: 'select count(*) as number from fff_staging.coastal_forecast_location where coastal_type = \'Multivariate Thresholds\'',
    insertPreparedStatement: `
      insert into 
        fff_staging.coastal_forecast_location (fffs_loc_id, coastal_order, centre, mfdo_area, ta_name, coastal_type, fffs_loc_name, location_x, location_y)
      values 
        (@fffs_loc_id, @coastal_order, @centre, @mfdo_area, @ta_name, @coastal_type, @fffs_loc_name, @location_x, @location_y)`,
    functionSpecificData: []
  }
  const refreshData = Object.assign(localRefreshData, commonRefreshData.commonCoastalLocationRefreshData)
  refreshData.functionSpecificData.push(...commonRefreshData.commonCoastalLocationFunctionSpecificData)
  refreshData.functionSpecificData.push(...commonRefreshData.commonCoastalMVTTritonLocationFunctionSpecificData)
  await refresh(context, refreshData)
}
