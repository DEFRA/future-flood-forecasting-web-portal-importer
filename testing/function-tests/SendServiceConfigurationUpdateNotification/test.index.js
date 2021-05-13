import Context from '../mocks/defaultContext.js'
import sendServiceConfigurationUpdateNotification from '../../../SendServiceConfigurationUpdateNotification/index.mjs'
import axios from 'axios'

jest.mock('axios')
export const sendServiceConfigurationUpdateNotificationTests = () => describe('Tests for replaying messages on the ProcessFewsEventCode dead letter queue', () => {
  const ORIGINAL_ENV = process.env
  let context

  describe('Message processing for the SendServiceConfigurationUpdateNotification dead letter queue', () => {
    beforeEach(async () => {
      // As mocks are reset and restored between each test (through configuration in package.json), the Jest mock
      // function implementation for the function context needs creating for each test.
      context = new Context()
    })

    afterEach(async () => {
      process.env = { ...ORIGINAL_ENV }
    })

    it('should send a service configuration update notification when core engine message processing is disabled explicitly', async () => {
      process.env['AzureWebJobs.ProcessFewsEventCode.Disabled'] = 'true'
      process.env['AzureWebJobs.ImportFromFews.Disabled'] = 'true'

      axios.mockReturnValue({
        data: {},
        status: 200,
        statusText: 'OK'
      })
      await sendServiceConfigurationUpdateNotification(context, '{"input": "notify"}')
      expect(axios.mock.calls.length).toBe(1)
    })

    it('should send a service configuration update notification when core engine message processing is disabled partially', async () => {
      process.env['AzureWebJobs.ProcessFewsEventCode.Disabled'] = 'true'
      process.env['AzureWebJobs.ImportFromFews.Disabled'] = 'false'

      axios.mockReturnValueOnce({
        data: {},
        status: 200,
        statusText: 'OK'
      })
      await sendServiceConfigurationUpdateNotification(context, '{"input": "notify"}')
      expect(axios.mock.calls.length).toBe(1)
    })

    it('should send a service configuration update notification when partial loading processing is disabled', async () => {
      process.env['AzureWebJobs.ProcessFewsEventCode.Disabled'] = 'false'
      process.env['AzureWebJobs.ImportFromFews.Disabled'] = 'true'

      axios.mockReturnValueOnce({
        data: {},
        status: 200,
        statusText: 'OK'
      })
      await sendServiceConfigurationUpdateNotification(context, '{"input": "notify"}')
    })

    it('should not send a service configuration update notification when core engine message processing is enabled explictly', async () => {
      process.env['AzureWebJobs.ProcessFewsEventCode.Disabled'] = 'false'
      process.env['AzureWebJobs.ImportFromFews.Disabled'] = 'false'
      await sendServiceConfigurationUpdateNotification(context, '{"input": "notify"}')
      expect(axios.mock.calls.length).toBe(0)
    })

    it('should not send a service configuration update notification when core engine message processing is enabled implicitly', async () => {
      axios.mockReturnValue({
        data: {},
        status: 200,
        statusText: 'OK'
      })
      await sendServiceConfigurationUpdateNotification(context, '{"input": "notify"}')
      expect(axios.mock.calls.length).toBe(0)
    })
  })
})
