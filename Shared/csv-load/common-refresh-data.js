module.exports =
{
  commonCoastalLocationRefreshData: Object.freeze({
    tableName: 'coastal_forecast_location'
  }),
  commonCoastalLocationFunctionSpecificData: Object.freeze([
    { tableColumnName: 'FFFS_LOC_ID', tableColumnType: 'NVarChar', expectedCSVKey: 'FFFSLocID' },
    { tableColumnName: 'COASTAL_ORDER', tableColumnType: 'Int', expectedCSVKey: 'CoastalOrder' },
    { tableColumnName: 'CENTRE', tableColumnType: 'NVarChar', expectedCSVKey: 'Centre' },
    { tableColumnName: 'COASTAL_TYPE', tableColumnType: 'NVarChar', expectedCSVKey: 'Type' }
  ])
}
