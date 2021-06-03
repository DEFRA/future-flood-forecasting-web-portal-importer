import { getFluvialDisplayGroupMetadata } from '../Shared/csv-load/display-group-helpers/display-group-metadata.js'
import refresh from '../Shared/csv-load/shared-refresh-csv-rows.js'

export default async function (context) {
  const refreshData = await getFluvialDisplayGroupMetadata(context)
  await refresh(context, refreshData)
}
