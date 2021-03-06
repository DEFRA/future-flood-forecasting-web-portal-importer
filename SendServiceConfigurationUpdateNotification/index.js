const axios = require('axios')
const { shouldServiceConfigurationUpdateNotificationBeSent } = require('../Shared/csv-load/service-configuration-update-utils')
module.exports = async function (context, message) {
  if (shouldServiceConfigurationUpdateNotificationBeSent(context)) {
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
