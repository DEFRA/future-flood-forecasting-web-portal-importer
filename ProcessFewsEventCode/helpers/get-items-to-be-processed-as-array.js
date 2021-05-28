module.exports = async function (records) {
  const array = []
  for (const record of records) {
    array.push({
      sourceId: record.source_id,
      sourceType: record.source_type
    })
  }
  return array
}
