if (process.env['TEST_TIMEOUT']) {
  jest.setTimeout(parseInt(process.env['TEST_TIMEOUT']))
}

describe('Run all unit tests in sequence', () => {
  const OLD_ENV = process.env

  beforeEach(() => {
    jest.resetModules() // Resets the module registry - the cache of all required modules.
    process.env = { ...OLD_ENV }
  })

  afterEach(() => {
    process.env = OLD_ENV
  })

  expect.extend({
    toTimeout (error) {
      if (error === 'EREQUEST' || 'ETIMEOUT') {
        return {
          pass: true,
          message: () => 'Failed to match the error code provided.'
        }
      }
    }
  })

  require('../RefreshDisplayGroupData/test.index')
  require('../RefreshForecastLocationData/test.index')
  require('../ImportTimeseriesRouter/test.timeseriesNonDisplayGroup.index')
  require('../ImportTimeseriesRouter/test.timeseriesDisplayGroup.index')
})
