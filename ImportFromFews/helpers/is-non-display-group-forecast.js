const timeseriesTypeConstants = require('./timeseries-type-constants')

module.exports = function (context, taskRunData) {
  return taskRunData.filterData && (taskRunData.filterData.timeseriesType === timeseriesTypeConstants.SIMULATED_FORECASTING || taskRunData.filterData.timeseriesType === timeseriesTypeConstants.EXTERNAL_FORECASTING)
}
