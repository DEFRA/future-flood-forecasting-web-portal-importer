module.exports =
{
  commonCoastalLocationRefreshData: Object.freeze({
    tableName: 'coastal_forecast_location'
  }),
  commonCoastalLocationFunctionSpecificData: Object.freeze([
    { tableColumnName: 'FFFS_LOC_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
    { tableColumnName: 'COASTAL_ORDER', tableColumnType: 'Int', expectedCSVKey: 'CoastalOrder' },
    { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
    { tableColumnName: 'COASTAL_TYPE', tableColumnType: 'NVarChar', expectedCSVKey: 'Type' },
    { tableColumnName: 'LOCATION_X', tableColumnType: 'Int', expectedCSVKey: 'LocationX' },
    { tableColumnName: 'LOCATION_Y', tableColumnType: 'Int', expectedCSVKey: 'LocationY' },
    { tableColumnName: 'FFFS_LOC_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName', nullValueOverride: true }
  ]),
  commonCoastalMVTTritonLocationFunctionSpecificData: Object.freeze([
    { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' },
    { tableColumnName: 'TA_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'TAName' }
  ]),
  returnNullForNaN: function (value) {
    if (isNaN(value)) {
      return null
    } else {
      return value
    }
  }
}
