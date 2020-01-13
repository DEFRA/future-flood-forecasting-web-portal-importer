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

  // A custom Jest matcher to test table timeouts
  expect.extend({
    toBeTimeoutError (error, tableName) {
      const pass = error.message === 'Lock request time out period exceeded.'
      // Note: this custom matcher returns a message for both cases (success and failure),
      // because it allows you to use .not. The test will fail with the corresponding
      // message depending on whether you want it to pass the validation (for example:
      // '.toBeTimeoutError()' OR '.not.toBeTimeoutError()').
      if (pass) {
        return {
          message: () => `Concerning table: ${tableName}. Expected received message: '${error.message}' to equal expected: 'Lock request time out period exceeded.'.`,
          pass: true
        }
      } else {
        return {
          message: () => `Concerning table: ${tableName}. Expected received message: '${error.message}' to equal expected: 'Lock request time out period exceeded.'.`,
          pass: false
        }
      }
    }
  })

  require('../RefreshDisplayGroupData/test.index')
  require('../RefreshNonDisplayGroupData/test.index')
  require('../RefreshForecastLocationData/test.index')
  require('../RefreshIgnoredWorkflowData/test.index')
  require('../ImportTimeseriesRouter/test.timeseriesNonDisplayGroup.index')
  require('../ImportTimeseriesRouter/test.timeseriesDisplayGroup.index')
})
