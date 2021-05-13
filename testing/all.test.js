import { afterAll, afterEach, beforeEach, describe, expect, vi } from 'vitest'
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
import { refreshLocationThresholdsDataTests } from './function-tests/RefreshLocationThresholdsData/test.index.js'
import { refreshThresholdGroupsDataTests } from './function-tests/RefreshThresholdGroupsData/test.index.js'
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
import { serviceBusHelperTests } from './function-tests/shared/test.service-bus-helper.js'
import { transactionHelperTests } from './function-tests/shared/test.transaction-helper.js'
import { sharedConnectionTests } from './function-tests/shared/test.connection-analysis.index.js'
import { invalidEnvironmentVariableBasedConfigurationTests } from './function-tests/shared/test.invalid-environment-variable-based-configuration.js'
import { msiDatabaseAuthenticationTests } from './function-tests/shared/test.msi-database-authentication.js'
import { connectionPoolManagementTests } from './function-tests/shared/test.connection-pool-management.index.js'

vi.mock('axios')
vi.mock('@azure/service-bus')
vi.mock('node-fetch')

if (process.env.TEST_TIMEOUT) {
  vi.setConfig({ testTimeout: parseInt(process.env.TEST_TIMEOUT) })
}

describe('Run all unit tests in sequence', () => {
  // In unit tests, use a small delay before throwing an error
  // following a transaction failure that could cause message replay.
  process.env.PAUSE_BEFORE_POTENTIAL_MESSAGE_REPLAY_MILLIS = '500'

  // In unit tests, use a small delay before propagating message
  // publication errors.
  process.env.PAUSE_BEFORE_PROPAGATING_MESSAGE_PUBLICATION_ERROR_MILLIS = '200'

  // Configure a custom PI Server call timeout to increase test coverage.
  // This will not be used by PI Server invocations made during unit tests
  // as PI Server responses are mocked.
  process.env.PI_SERVER_CALL_TIMEOUT = '10000'

  const ORIGINAL_ENV = process.env

  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV }
    vi.resetAllMocks()
  })

  afterEach(() => {
    process.env = ORIGINAL_ENV
  })

  afterAll(async () => {
    // When all tests run successfully, the connection pool used by the function app will be closed
    // already. Attempt to close it again to increase test coverage.
    await transactionHelper.closeConnectionPool()
  })

  // A custom matcher to test table timeouts
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
  refreshLocationThresholdsDataTests()
  refreshThresholdGroupsDataTests()
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
  serviceBusHelperTests()
  transactionHelperTests()
  sharedConnectionTests()
  invalidEnvironmentVariableBasedConfigurationTests()
  msiDatabaseAuthenticationTests()
  // Run connection pool management tests last as the connection pool closure function is tested.
  // This resets module variables that cause subsequent tests using pooled connection based queries
  // to fail even if modules are reset by Vitest. Resetting modules does not appear to cause Vitest
  // to run the IIFE that initialises the connection pool.
  connectionPoolManagementTests()
})
