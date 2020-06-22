if (process.env['TEST_TIMEOUT']) {
  jest.setTimeout(parseInt(process.env['TEST_TIMEOUT']))
}

describe('Run all unit tests in sequence', () => {
  const OLD_ENV = process.env

  beforeEach(() => {
    process.env = { ...OLD_ENV }
  })

  afterEach(() => {
    process.env = OLD_ENV
  })

  // A custom Jest matcher to test table timeouts
  expect.extend({
    toBeTimeoutError (error, tableName) {
      const pass = error.message === ('Lock request time out period exceeded.')
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

  require('../DeleteExpiredTimeseries/test.index')
  require('../RefreshFluvialDisplayGroupData/test.index')
  require('../RefreshCoastalDisplayGroupData/test.index')
  require('../RefreshNonDisplayGroupData/test.index')
  require('../RefreshIgnoredWorkflowData/test.index')
  require('../RefreshFluvialForecastLocationData/test.index')
  require('../RefreshCoastalTidalForecastLocationData/test.index')
  require('../RefreshCoastalTritonForecastLocationData/test.index')
  require('../RefreshCoastalMVTForecastLocationData/test.index')
  require('../ImportTimeseriesRouter/test.forecastFlags.index')
  require('../ImportTimeseriesRouter/test.timeseriesNonDisplayGroup.index')
  require('../ImportTimeseriesRouter/test.timeseriesFluvialDisplayGroup.index')
  require('../ImportTimeseriesRouter/test.timeseriesCoastalDisplayGroup.index')
  require('../ImportTimeseriesRouter/test.timeseriesIgnoredWorkflow')
  require('../Shared/test.connection-analysis.index')
})
