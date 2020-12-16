const getPiServerErrorMessage = require('../../Shared/timeseries-functions/get-pi-server-error-message')
const axios = require('axios')
module.exports = async function (context) {
  try {
    const fewsPiUrl =
      encodeURI(`${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/filters?documentFormat=PI_JSON`)
    await axios.get(fewsPiUrl)
  } catch (err) {
    if (typeof err.response === 'undefined') {
      context.log.error('PI Server is unavailable')
    } else {
      const piServerErrorMessage = getPiServerErrorMessage(context, err)
      context.log.error(`An unexpected error occurred when checking if the PI Server is available - ${err.message} (${piServerErrorMessage})`)
    }
    // Attempt message replay.
    throw err
  }
}
