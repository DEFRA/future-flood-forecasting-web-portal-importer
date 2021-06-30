const axios = require('axios')
module.exports = async function (context, message) {
  if (JSON.parse(process.env['AzureWebJobs.ProcessFewsEventCode.Disabled'] || false) ||
      JSON.parse(process.env['AzureWebJobs.ImportFromFews.Disabled'] || false)) {
    const options = {
      method: 'post',
      url: process.env.SERVICE_CONFIGURATION_UPDATE_NOTIFICATION_URL,
      data: {}
    }
    context.log('Sending service configuration update notification')
    await axios(options)
  } else {
    context.log('Ignoring service configuration update notification - core engine message processing is enabled')
  }
}
