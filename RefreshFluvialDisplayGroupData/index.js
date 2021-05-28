const displayGroupMetadata = require('../Shared/csv-load/display-group-helpers/display-group-metadata')
const refresh = require('../Shared/csv-load/shared-refresh-csv-rows')

module.exports = async function (context) {
  const refreshData = await displayGroupMetadata.getFluvialDisplayGroupMetadata(context)
  await refresh(context, refreshData)
}
