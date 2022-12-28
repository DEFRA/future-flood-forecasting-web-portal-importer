module.exports = function (context, isoTimeString, parameterName) {
  // The database in which data is loaded requires fractional seconds to be included in dates. By contrast
  // the REST interface of the core forecasting engine requires fractional seconds to be excluded from dates.
  return `&${parameterName}=${isoTimeString.substring(0, 19)}Z`
}
