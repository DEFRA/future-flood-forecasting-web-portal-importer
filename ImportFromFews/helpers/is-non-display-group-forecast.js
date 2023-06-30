import { timeseriesTypeConstants } from './timeseries-type-constants.js'

export default function (context, taskRunData) {
  return taskRunData.filterData && (taskRunData.filterData.timeseriesType === timeseriesTypeConstants.SIMULATED_FORECASTING || taskRunData.filterData.timeseriesType === timeseriesTypeConstants.EXTERNAL_FORECASTING)
}
