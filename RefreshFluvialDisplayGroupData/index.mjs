import displayGroupMetadata from '../Shared/csv-load/display-group-helpers/display-group-metadata.js'
import refresh from '../Shared/csv-load/shared-refresh-csv-rows.js'

module.exports = async function (context) {
  const refreshData = await displayGroupMetadata.getFluvialDisplayGroupMetadata(context)
  await refresh(context, refreshData)
}
