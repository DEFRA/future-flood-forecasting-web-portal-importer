# Non-Test Function App Settings/Environment Variables

## Mandatory Build Time Environment Variables

| name                                      | description                                                                                           |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------|
| AZURE_SERVICE_BUS_MAX_CONCURRENT_CALLS    | The maximum number of concurrent calls from Azure Service Bus that are permitted (1 to 10 inclusive). |
| DELETE_EXPIRED_TIMESERIES_CRON_EXPRESSION | The expression dictating how often stale timeseries data is removed from staging.                     |

## Optional Build Time Environment Variables

| name                                      | description                                                                                           |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------|
| IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED | When set to true, this provides an output binding connecting to an azure service bus queue named 'fews-staged-timeseries-queue'|

## Redundant Legacy Build Time Function App Settings/Environment Variables

The function app settings/environment variables below are no longer used. It is recommended that they should be removed from any existing installation
accordingly.

| name                                      | description                                                                                           |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------|
| FFFS_WEB_PORTAL_BUILD_TYPE                | **queue** or **topic** (configures the function app to use either Azure service bus queues or topics) |

## Mandatory Runtime Function App Settings/Environment Variables For Deployments Affecting The Structure Of Core Forecasting Engine Workflow Configuration Data

| name                                      | description                                                                                           |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------|
| AzureWebJobs.ImportFromFews.Disabled             | Disable the ImportFromFews function (Set to true)                                              |
| AzureWebJobs.ProcessFewsEventCode.Disabled       | Disable the ProcessFewsEventCode function (Set to true)                                        |

The ImportFromFews and ProcessFewsEventCode functions **must** be disabled during deployments affecting the structure of core forecasting engine workflow configuration data. The endpoints **must** remain disabled until messages on the following Azure Service Bus queues have been processed **successfully**.

* fews-coastal-display-group-queue
* fews-fluvial-display-group-queue
* fews-ignored-workflows-queue
* fews-non-display-group-queue

This prevents core forecasting engine messages from being processed until supporting configuration data has been loaded.

## Mandatory Runtime Function App Settings/Environment Variables

### Mandatory Staging Database Authentication Related Function App Settings/Environment Variables

**Either** Microsoft Azure Managed Service Identity (MSI) App Service authentication or username/password authentication is supported. Note that MSI authentication is dependent on a number of prerequisites:

* The staging database **must** include an account for a Microsoft Azure Active Directory user with the same name as the Microsoft Azure function app that the functions are published to.
* The functions **must** be published to a function app with a system identity that has role permissions on the staging database.

Note that the list of prerequisites is simplified and excludes fine detail accordingly. The reader is encouraged
to consult MSI documentation before configuring the prerequisites as a number of options are available. Example MSI documentation can be found at [https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=dotnet](https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=dotnet)

#### MSI App Service Authentication Related Function App Settings/Environment Variables

Note that these environment variables are provided by the Microsoft Azure platform automatically when the functions are published to a function app
with a system identity.

| name                                             | description                                                                                    |
|--------------------------------------------------|------------------------------------------------------------------------------------------------|
| MSI_ENDPOINT                                     | [Azure managed identity](https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=dotnet) URL to the local token service |
| MSI_SECRET                                       | [Azure managed identity](https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=dotnet) header used to help mitigate server-side request forgery (SSRF) attacks.  |

#### Username/Password Authentication Related Function App Settings/Environment Variables

| name                                             | description                                                                                    |
|--------------------------------------------------|------------------------------------------------------------------------------------------------|
| SQLDB_USER                                       | [mssql node module](https://www.npmjs.com/package/mssql) username for authentication           |
| SQLDB_PASSWORD                                   | [mssql node module](https://www.npmjs.com/package/mssql) password for authentication           |

### Mandatory Non-Authentication Related App Settings/Environment Variables

| name                                             | description                                                                                    |
|--------------------------------------------------|------------------------------------------------------------------------------------------------|
| APPINSIGHTS_INSTRUMENTATIONKEY                   | Instrumentation key controlling if telemetry is sent to the ApplicationInsights service          |
| AzureWebJobsServiceBus                           | Service bus connection string used by the function app                                         |
| AzureWebJobsStorage                              | Storage account connection string used by the function app                                     |
| AZURE_STORAGE_CONNECTION_STRING                  | Storage account connection string used by the function app                                     |
| FEWS_PI_API                                      | Protocol, fully qualified domain name and optional port of the core forecasting engine REST API|
| FUNCTIONS_EXTENSION_VERSION                      | Functions runtime version (**must be ~2**)                                                     |
| FUNCTIONS_WORKER_RUNTIME                         | The language worker runtime to load in the function app (**must be node**)                     |
| WEBSITE_NODE_DEFAULT_VERSION                     | Default version of Node.js (**Microsoft Azure default is recommended**)                        |
| FLUVIAL_FORECAST_LOCATION_URL                    | URL used to provide the forecast location data                                                 |
| COASTAL_TRITON_FORECAST_LOCATION_URL             | URL used to provide the coastal triton location data                                           |
| COASTAL_TIDAL_FORECAST_LOCATION_URL              | URL used to provide the coastal tidal location data                                            |
| COASTAL_MVT_FORECAST_LOCATION_URL                | URL used to provide the coastal mvt location data                                              |
| FLUVIAL_DISPLAY_GROUP_WORKFLOW_URL               | URL used to provide the fluvial display groups workflow reference data                         |
| COASTAL_DISPLAY_GROUP_WORKFLOW_URL               | URL used to provide the coastal display groups workflow reference data                         |
| NON_DISPLAY_GROUP_WORKFLOW_URL                   | URL used to provide the non display groups workflow reference data                             |
| IGNORED_WORKFLOW_URL                             | URL used to provide the ignored workflows                                                      |
| DELETE_EXPIRED_TIMESERIES_HARD_LIMIT             | The number of hours before the current time before which all timeseries data should be removed |
| MVT_URL                                          | URL used to provide the multivariate threshold information                                     |
| AzureWebJobs.ReplayImportFromFews.Disabled       | Disable the ReplayImportFromFews function by default (set to true)                             |
| AzureWebJobs.ReplayProcessFewsEventCode.Disabled | Disable the ReplayProcessFewsEventCode function by default (set to true)                       |
| SQLDB_SERVER | [mssql node module](https://www.npmjs.com/package/mssql) server |
| SQLDB_DATABASE | [mssql node module](https://www.npmjs.com/package/mssql) database name |

## Redundant Legacy Runtime Function App Settings/Environment Variables

The function app settings/environment variables below are no longer used. It is recommended that they should be removed from any existing installation
accordingly.

| name                                      | description                                                                                                |
|-------------------------------------------|------------------------------------------------------------------------------------------------------------|
| FEWS_INITIAL_LOAD_HISTORY_HOURS           | Number of hours before the initial import time that core forecasting engine data should be retrieved for   |
| FEWS_LOAD_HISTORY_HOURS                   | Number of hours before subsequent import times that core forecasting engine data should be retrieved for   |
| FEWS_IMPORT_DISPLAY_GROUPS_SCHEDULE       | UNIX Cron expression controlling when time series display groups are imported                              |
| LOCATION_LOOKUP_URL                       | URL used to provide location lookup data associated with display groups                                    |
| AZURE_SERVICE_BUS_LOCATION_LOOKUP_SUBSCRIPTION_NAME | Subscription name associated with fews-location-lookup-topic                                     |
| AZURE_SERVICE_BUS_DISPLAY_GROUP_SUBSCRIPTION_NAME | Subscription name associated with fews-display-group-topic (no fluvial/coastal distinction)        |
| FEWS_LOCATION_IDS                         | Semi-colon separated list of locations used with scheduled imports                                         |
| FEWS_PLOT_ID                              | The core forecasting engine plot ID used with scheduled imports                                            |
| AZURE_SERVICE_BUS_FLUVIAL_NON_DISPLAY_GROUP_SUBSCRIPTION_NAME | Subscription name associated with fews-non-display-group-topic                         |
| AZURE_SERVICE_BUS_FORECAST_LOCATION_SUBSCRIPTION_NAME | Subscription name associated with fews-forecast-location-topic                                 |
| FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA | Staging, staging schema name                                                                               |
| FFFS_WEB_PORTAL_STAGING_DB_REPORTING_SCHEMA | Staging, reporting schema name                                                                           |
| SQLDB_CONNECTION_STRING                          | [mssql node module](https://www.npmjs.com/package/mssql) connection string |
| AZURE_SERVICE_BUS_EVENT_CODE_SUBSCRIPTION_NAME        | Subscription name associated with fews-eventcode-topic                                         |
| AZURE_SERVICE_BUS_FEWS_IMPORT_SUBSCRIPTION_NAME       | Subscription name associated with fews-import-topic                                            |
| AZURE_SERVICE_BUS_STAGED_TIMESERIES_SUBSCRIPTION_NAME | Subscription name associated with fews-staged-timeseries-topic                                 |
| AZURE_SERVICE_BUS_FLUVIAL_DISPLAY_GROUP_SUBSCRIPTION_NAME | Subscription name associated with fews-fluvial-display-group-topic                         |
| AZURE_SERVICE_BUS_COASTAL_DISPLAY_GROUP_SUBSCRIPTION_NAME | Subscription name associated with fews-coastal-display-group-topic                         |
| AZURE_SERVICE_BUS_NON_DISPLAY_GROUP_SUBSCRIPTION_NAME | Subscription name associated with fews-non-display-group-topic (no fluvial/coastal distinction)|
| AZURE_SERVICE_BUS_FLUVIAL_FORECAST_LOCATION_SUBSCRIPTION_NAME | Subscription name associated with fews-fluvial-forecast-location-topic                 |
| AZURE_SERVICE_BUS_COASTAL_TIDAL_FORECAST_LOCATION_SUBSCRIPTION_NAME | Subscription name associated with fews-coastal-tidal-forecast-location-topic     |
| AZURE_SERVICE_BUS_COASTAL_TRITON_FORECAST_LOCATION_SUBSCRIPTION_NAME | Subscription name associated with fews-coastal-triton-forecast-location-topic   |
| AZURE_SERVICE_BUS_COASTAL_MVT_FORECAST_LOCATION_SUBSCRIPTION_NAME | Subscription name associated with fews-coastal-mvt-forecast-location-topic         |
| AZURE_SERVICE_BUS_IGNORED_WORKFLOWS_SUBSCRIPTION_NAME | Subscription name associated with fews-ignored-workflows-topic                                 |
| AZURE_SERVICE_BUS_MVT_SUBSCRIPTION_NAME | Subscription name associated with fews-mvt-topic                                                             |
| DELETE_EXPIRED_TIMESERIES_SOFT_LIMIT | The number of hours before the current time before which all completed status timeseries data should be removed (defaults to DELETE_EXPIRED_TIMESERIES_HARD_LIMIT)|

## Optional Runtime Function App Settings/Environment Variables

| name                         | description                                                                                                                                                                                                                    |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| AUTHENTICATE_WITH_MSI | A boolean controlling whether or not MSI App Service authentication is enabled |
| SQLDB_PORT | [mssql node module](https://www.npmjs.com/package/mssql) database port (1024 to 49151 inclusive - uses the mssql module default))
| SQLDB_CONNECTION_TIMEOUT_MILLIS | [mssql node module](https://www.npmjs.com/package/mssql) database connection timeout (15000 to 60000 inclusive - uses the mssql module default) |
| SQLDB_REQUEST_TIMEOUT_MILLIS | [mssql node module](https://www.npmjs.com/package/mssql) database request timeout (15000 to 120000 inclusive - defaults to 60000ms) |
| SQLDB_MAX_RETRIES_ON_TRANSIENT_ERRORS | [mssql node module](https://www.npmjs.com/package/mssql) maximum number of connection retries for transient errors (3 to 20 inclusive - uses the underlying [tedious module](http://tediousjs.github.io/tedious/api-connection.html) default |
| SQLDB_PACKET_SIZE | [mssql node module](https://www.npmjs.com/package/mssql) database packet size (a power of two between 4096 and 65536 inclusive - uses the underlying [tedious module](http://tediousjs.github.io/tedious/api-connection.html) default |
| SQLDB_ABORT_TRANSACTION_ON_ERROR | [mssql node module](https://www.npmjs.com/package/mssql) boolean determining whether to rollback a transaction automatically if any error is encountered during the given transaction's execution. This sets the value for XACT_ABORT during the initial SQL phase of a connection (uses the mssql module default.) |
| SQLDB_MAX_POOLED_CONNECTIONS | [mssql node module](https://www.npmjs.com/package/mssql) maximum connection pool size (1 to 20 inclusive - defaults to AZURE_SERVICE_BUS_MAX_CONCURRENT_CALLS * 2) |
| SQLDB_MIN_POOLED_CONNECTIONS  | [mssql node module](https://www.npmjs.com/package/mssql) minimum connection pool size (1 to 20 inclusive - defaults to AZURE_SERVICE_BUS_MAX_CONCURRENT_CALLS + 1) |
| SQLDB_ACQUIRE_TIMEOUT_MILLIS | [mssql node module](https://www.npmjs.com/package/mssql) database resource acquisition timeout (5000 to 120000 inclusive - uses the underlying [tarn module](https://www.npmjs.com/package/tarn) acquireTimeoutMillis default) |
| SQLDB_CREATE_TIMEOUT_MILLIS | [mssql node module](https://www.npmjs.com/package/mssql) creation operation timeout if a resource cannot be acquired (5000 to 120000 inclusive- uses the underlying [tarn module](https://www.npmjs.com/package/tarn) createTimeoutMillis default) |
| SQLDB_DESTROY_TIMEOUT_MILLIS | [mssql node module](https://www.npmjs.com/package/mssql) destroy operation timeout (5000 to 30000 inclusive - uses the underlying [tarn module](https://www.npmjs.com/package/tarn) destroyTimeoutMillis default) |
| SQLDB_IDLE_TIMEOUT_MILLIS | [mssql node module](https://www.npmjs.com/package/mssql) idle resource timeout (5000 to 120000 inclusive- uses the underlying [tarn module](https://www.npmjs.com/package/tarn) idleTimeoutMillis default) |
| SQLDB_REAP_INTERVAL_MILLIS | [mssql node module](https://www.npmjs.com/package/mssql) interval to check for idle resources to destroy (1000 to 30000 inclusive - uses the underlying [tarn module](https://www.npmjs.com/package/tarn) reapIntervalMillis default) |
| SQLDB_CREATE_RETRY_INTERVAL_MILLIS | [mssql node module](https://www.npmjs.com/package/mssql) interval to wait before retrying a failed creation operation (200 to 5000 inclusive - uses the underlying [tarn module](https://www.npmjs.com/package/tarn) createRetryIntervalMillis default) |
| SQLDB_PROPAGATE_CREATE_ERROR | [mssql node module](https://www.npmjs.com/package/mssql) boolean determining if the first pending acquire is rejected when a create operation fails. If this is false (the default) then the create operation is retried until the acquisition timeout has passed (uses the mssql module default.) |
| SQLDB_TRUST_SERVER_CERTIFICATE | [mssql node module](https://www.npmjs.com/package/mssql) boolean determining if server certificates should be trusted. If this is false (the default) certificate issues will prevent connectivity (uses the mssql module default.) This **must** be set to true
| when a containerised SQL Server instance is used for unit testing. |
| SQLDB_LOCK_TIMEOUT | Time limit for database lock acquisition in milliseconds (defaults to 6500ms)                                                                                                                                                  |
| FEWS_DISPLAY_GROUP_START_TIME_OFFSET_HOURS | The number of hours before task run completion time that core forecasting engine display group data should be retrieved (defaults to 14)                                                                                                     |
| FEWS_DISPLAY_GROUP_END_TIME_OFFSET_HOURS   | The number of hours after task run completion time that core forecasting engine display group data should be retrieved (defaults to 120)                                                                                                     |
| FEWS_NON_DISPLAY_GROUP_OFFSET_HOURS | The number of hours before the previous task run end time (current task run start time if its the first task run for a given workflow) that core engine non-display-group data should be retrieved for (defaults to 24) |
| FEWS_MAXIMUM_NON_DISPLAY_GROUP_CREATION_OFFSET_HOURS | The maximum number of hours between core engine task runs for certain non-display group data workflows before the previous task run is considered obsolete (defaults to 48). If the limit is exceeded, data will be retrieved for the configured number of hours before the start of the current task run instead of referencing the completion time of the previous task run. |
| FEWS_NON_DISPLAY_GROUP_END_CREATION_OFFSET_SECONDS | The number of seconds to add to the endCreationTime parameter when included in calls to the core forecasting engine REST API (defaults to 5) |
| PI_SERVER_CALL_DELAY_MILLIS | The number of milliseconds to delay each data retrieval call to the
core forecasting engine REST API for external historical timeseries (The REST API is provided by the PI Server) by (defaults to 2000) |
| TIMESERIES_DELETE_BATCH_SIZE | The number of timeseries_header rows (including linked foreign key rows) to be deleted on a single run of the DeleteExpiredTimeseries function (defaults to 1000)                                                               |
| CONFIG_AUTHORIZATION | If resources specified through URL related app settings are protected, this app setting must be used to provide access. The value provides access to a token used in a HTTP Authorization header. |
| SERVICE_CONFIG_UPDATE_DETECTION_LIMIT | The maximum number of seconds (in total) since all or workflow configuration data has been updated for a core forecasting engine service configuration update to be detected (defaults to 300). Detection for all configuration data causes a message to be placed on **fews-service-configuration-update-completed-queue**. Detection for workflow configuration data causes replay of failed messages not caused by workflow CSV related configuration errors. |
| SERVICE_CONFIGURATION_UPDATE_NOTIFICATION_URL | The URL that service configuration update notifications should be sent to. |
