module.exports = async function (context, errorMessage) {
  let matches
  const regex = /Location\s+(.*?)\s+does not exist/ig
  const nonExistentLocationsIds = new Set()

  while ((matches = regex.exec(errorMessage)) != null) {
    nonExistentLocationsIds.add(matches[1])
  }

  return nonExistentLocationsIds
}
