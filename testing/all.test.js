if (process.env['TEST_TIMEOUT']) {
  jest.setTimeout(parseInt(process.env['TEST_TIMEOUT']))
}

describe('Run all unit tests in sequence', () => {
  const OLD_ENV = process.env

  beforeEach(() => {
    process.env = { ...OLD_ENV }
    jest.resetAllMocks()
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

  require('./function-tests/DeleteExpiredTimeseries/test.index')
  require('./function-tests/RefreshFluvialDisplayGroupData/test.index')
  require('./function-tests/RefreshCoastalDisplayGroupData/test.index')
  require('./function-tests/RefreshNonDisplayGroupData/test.index')
  require('./function-tests/RefreshIgnoredWorkflowData/test.index')
  require('./function-tests/RefreshFluvialForecastLocationData/test.index')
  require('./function-tests/RefreshCoastalTidalForecastLocationData/test.index')
  require('./function-tests/RefreshCoastalTritonForecastLocationData/test.index')
  require('./function-tests/RefreshCoastalMVTForecastLocationData/test.index')
  require('./function-tests/ProcessFewsEventCode/test.timeseriesNonDisplayGroup.index')
  require('./function-tests/ProcessFewsEventCode/test.timeseriesFluvialDisplayGroup.index')
  require('./function-tests/ProcessFewsEventCode/test.timeseriesCoastalDisplayGroup.index')
  require('./function-tests/ProcessFewsEventCode/test.timeseriesIgnoredWorkflow')
  require('./function-tests/ProcessFewsEventCode/test.forecastFlags.index')
  require('./function-tests/ImportFromFews/test.timeseriesNonDisplayGroup.index')
  require('./function-tests/ImportFromFews/test.timeseriesFluvialDisplayGroup.index')
  require('./function-tests/ImportFromFews/test.timeseriesCoastalDisplayGroup.index')
  require('./function-tests/ImportFromFews/test.timeseriesIgnoredWorkflow.index')
  require('./function-tests/shared/test.connection-analysis.index')
})
