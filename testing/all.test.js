import { jest } from '@jest/globals'
import 'regenerator-runtime/runtime'
import * as transactionHelper from '../Shared/transaction-helper.js'
import { timeseriesDataDeletionTests } from './function-tests/DeleteExpiredTimeseries/test.index.js'
import { refreshFluvialDisplayGroupWorkflowDataTests } from './function-tests/RefreshFluvialDisplayGroupData/test.index.js'
import { refreshCoastalDisplayGroupWorkflowDataTests } from './function-tests/RefreshCoastalDisplayGroupData/test.index.js'
import { refreshNonDisplayGroupWorkflowDataTests } from './function-tests/RefreshNonDisplayGroupData/test.index.js'
import { refreshIgnoredWorkflowDataTests } from './function-tests/RefreshIgnoredWorkflowData/test.index.js'
import { refreshFluvialForecastLocationDataTests } from './function-tests/RefreshFluvialForecastLocationData/test.index.js'
import { refreshCoastalTidalForecastLocationDataTests } from './function-tests/RefreshCoastalTidalForecastLocationData/test.index.js'
import { refreshCoastalTritonForecastLocationDataTests } from './function-tests/RefreshCoastalTritonForecastLocationData/test.index.js'
import { refreshCoastalMVTForecastLocationDataTests } from './function-tests/RefreshCoastalMVTForecastLocationData/test.index.js'
import { refreshMVTDataTests } from './function-tests/RefreshMVTData/test.index.js'
import { sendServiceConfigurationUpdateNotificationTests } from './function-tests/SendServiceConfigurationUpdateNotification/test.index.js'
import { nonDisplayGroupProcessFewsEventCodeTests } from './function-tests/ProcessFewsEventCode/test.timeseriesNonDisplayGroup.index.js'
import { fluvialDisplayGroupProcessFewsEventCodeTests } from './function-tests/ProcessFewsEventCode/test.timeseriesFluvialDisplayGroup.index.js'
import { coastalDisplayGroupProcessFewsEventCodeTests } from './function-tests/ProcessFewsEventCode/test.timeseriesCoastalDisplayGroup.index.js'
import { ignoredWorkflowProcessFewsEventCodeTests } from './function-tests/ProcessFewsEventCode/test.timeseriesIgnoredWorkflow.js'
import { forecastFlagTests } from './function-tests/ProcessFewsEventCode/test.forecastFlags.index.js'
import { replayDeadLetteredProcessFewsEventCodeMessageTests } from './function-tests/ReplayProcessFewsEventCode/test.index.js'
import { nonDisplayGroupImportFromFewsTests } from './function-tests/ImportFromFews/test.timeseriesNonDisplayGroup.index.js'
import { fluvialDisplayGroupImportFromFewsTests } from './function-tests/ImportFromFews/test.timeseriesFluvialDisplayGroup.index.js'
import { coastalDisplayGroupImportFromFewsTests } from './function-tests/ImportFromFews/test.timeseriesCoastalDisplayGroup.index.js'
import { ignoredWorkflowImportFromFewsTests } from './function-tests/ImportFromFews/test.timeseriesIgnoredWorkflow.index.js'
import { replayDeadLetteredImportFromFewsMessageTests } from './function-tests/ReplayImportFromFews/test.index.js'
import { sharedConnectionTests } from './function-tests/shared/test.connection-analysis.index.js'
import { connectionPoolManagementTests } from './function-tests/shared/test.connection-pool-management.index.js'
import { invalidEnvironmentVariableBasedConfigurationTests } from './function-tests/shared/test.invalid-environment-variable-based-configuration.js'
import { msiDatabaseAuthenticationTests } from './function-tests/shared/test.msi-database-authentication.js'

if (process.env.TEST_TIMEOUT) {
  jest.setTimeout(parseInt(process.env.TEST_TIMEOUT))
}

describe('Run all unit tests in sequence', () => {
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

  timeseriesDataDeletionTests()
  refreshFluvialDisplayGroupWorkflowDataTests()
  refreshCoastalDisplayGroupWorkflowDataTests()
  refreshNonDisplayGroupWorkflowDataTests()
  refreshIgnoredWorkflowDataTests()
  refreshFluvialForecastLocationDataTests()
  refreshCoastalTidalForecastLocationDataTests()
  refreshCoastalTritonForecastLocationDataTests()
  refreshCoastalMVTForecastLocationDataTests()
  refreshMVTDataTests()
  sendServiceConfigurationUpdateNotificationTests()
  nonDisplayGroupProcessFewsEventCodeTests()
  fluvialDisplayGroupProcessFewsEventCodeTests()
  coastalDisplayGroupProcessFewsEventCodeTests()
  ignoredWorkflowProcessFewsEventCodeTests()
  forecastFlagTests()
  replayDeadLetteredProcessFewsEventCodeMessageTests()
  nonDisplayGroupImportFromFewsTests()
  fluvialDisplayGroupImportFromFewsTests()
  coastalDisplayGroupImportFromFewsTests()
  ignoredWorkflowImportFromFewsTests()
  replayDeadLetteredImportFromFewsMessageTests()
  sharedConnectionTests()
  connectionPoolManagementTests()
  invalidEnvironmentVariableBasedConfigurationTests()
  msiDatabaseAuthenticationTests()
})
