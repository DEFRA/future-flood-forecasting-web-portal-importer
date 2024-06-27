const transactionHelper = require('../Shared/transaction-helper')

if (process.env.TEST_TIMEOUT) {
  jest.setTimeout(parseInt(process.env.TEST_TIMEOUT))
}

describe('Run all unit tests in sequence', () => {
  // As the PI Server is mocked in unit tests, a small delay is required for the
  // amount of time to wait before checking if all filter based data for a task
  // run is available from the PI Server.
  process.env.CHECK_FOR_TASK_RUN_DATA_AVAILABILITY_DELAY_MILLIS = '100'

  // Configure a custom PI Server call timeout to increase test coverage.
  // This will not be used by PI Server invocations made during unit tests
  // as PI Server responses are mocked.
  process.env.PI_SERVER_CALL_TIMEOUT = '10000'

  const ORIGINAL_ENV = process.env

  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV }
    jest.resetAllMocks()
  })

  afterEach(() => {
    process.env = ORIGINAL_ENV
  })

  afterAll(async () => {
    // When all tests run successfully, the connection pool used by the function app will be closed
    // already. Attempt to close it again to increase test coverage.
    await transactionHelper.closeConnectionPool()
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
  require('./function-tests/RefreshMVTData/test.index')
  require('./function-tests/RefreshLocationThresholdsData/test.index')
  require('./function-tests/RefreshThresholdGroupsData/test.index')
  require('./function-tests/SendServiceConfigurationUpdateNotification/test.index')
  require('./function-tests/ProcessFewsEventCode/test.timeseriesNonDisplayGroup.index')
  require('./function-tests/ProcessFewsEventCode/test.timeseriesFluvialDisplayGroup.index')
  require('./function-tests/ProcessFewsEventCode/test.timeseriesCoastalDisplayGroup.index')
  require('./function-tests/ProcessFewsEventCode/test.timeseriesIgnoredWorkflow')
  require('./function-tests/ProcessFewsEventCode/test.forecastFlags.index')
  require('./function-tests/ReplayProcessFewsEventCode/test.index')
  require('./function-tests/ImportFromFews/test.timeseriesNonDisplayGroup.index')
  require('./function-tests/ImportFromFews/test.timeseriesFluvialDisplayGroup.index')
  require('./function-tests/ImportFromFews/test.timeseriesCoastalDisplayGroup.index')
  require('./function-tests/ImportFromFews/test.timeseriesIgnoredWorkflow.index')
  require('./function-tests/ReplayImportFromFews/test.index')
  require('./function-tests/shared/test.transaction-helper')
  require('./function-tests/shared/test.connection-analysis.index')
  require('./function-tests/shared/test.invalid-environment-variable-based-configuration')
  require('./function-tests/shared/test.msi-database-authentication')
  // Run connection pool management tests last as the connection pool closure function is tested.
  // This resets module variables that cause subsequent tests using pooled connection based queries
  // to fail even if modules are reset by Jest. Resetting modules does not appear to cause Jest
  // to run the IIFE that initialises the connection pool.
  require('./function-tests/shared/test.connection-pool-management.index')
})
