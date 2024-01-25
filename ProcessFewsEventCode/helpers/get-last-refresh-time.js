const getPiServerErrorMessage = require('../../Shared/timeseries-functions/get-pi-server-error-message')
const axios = require('axios')

module.exports = async function (context) {
  try {
    const URL = `${process.env.FEWS_PI_API}/FewsWebServices/rest/fewspiservice/v1/lastrefreshtime?documentFormat=PI_JSON`
    const response = await axios.get(URL)
    context.log('last refresh time response', response)
  } catch (err) {
    if (typeof err.response === 'undefined') {
      context.log('Pi Server not available')
    } else {
      const piServerErrorMessage = getPiServerErrorMessage(context, err)
      context.log.error(`An unexpected error occured while fetching last refreshtime ${err.message} (${piServerErrorMessage})`)
    }
    throw err
  }
}
