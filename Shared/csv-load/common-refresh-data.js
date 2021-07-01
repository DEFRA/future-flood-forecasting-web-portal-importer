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
    { tableColumnName: 'FFFS_LOC_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocName', nullValueOverride: true },
    { tableColumnName: 'LOCATION_X', tableColumnType: 'Decimal', expectedCSVKey: 'LocationX', precision: 38, scale: 8, preprocessor: returnNullForNaN },
    { tableColumnName: 'LOCATION_Y', tableColumnType: 'Decimal', expectedCSVKey: 'LocationY', precision: 38, scale: 8, preprocessor: returnNullForNaN }

  ]),
  commonCoastalMVTTritonLocationFunctionSpecificData: Object.freeze([
    { tableColumnName: 'MFDO_AREA', tableColumnType: 'NVarChar', expectedCSVKey: 'MFDOArea' },
    { tableColumnName: 'TA_NAME', tableColumnType: 'NVarChar', expectedCSVKey: 'TAName' }
  ]),
  returnNullForNaN
}

function returnNullForNaN (value) {
  if (isNaN(value)) {
    return null
  } else {
    return value
  }
}
