# Non-Test Function App Settings/Environment Variables

## Mandatory Build Time Environment Variables

| name                                      | description                                                                                             |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------|
| FFFS_WEB_PORTAL_BUILD_TYPE                | **queue** or **topic** (configures the function app to use either Azure service bus queues or topics)   |
| AZURE_SERVICE_BUS_MAX_CONCURRENT_CALLS    | The maximum number of concurrent calls from Azure Service Bus that are permitted.                       |

## Mandatory Runtime Function App Settings/Environment Variables

| name                                      | description                                                                                             |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------|
| APPINSIGHTS_INSTRUMENTATIONKEY            | Instrumention key controlling if telemetry is sent to the ApplicationInsights service                   |
| AzureWebJobsServiceBus                    | Service bus connection string used by the function app                                                  |
| AzureWebJobsStorage                       | Storage account connection string used by the function app                                              |
| AZURE_STORAGE_CONNECTION_STRING           | Storage account connection string used by the function app                                              |
| FEWS_PI_API                               | Protocol, fully qualified domain name and optional port of the core forecasting engine REST API         |
| FUNCTIONS_EXTENSION_VERSION               | Functions runtime version (**must be ~2**)                                                              |
| FUNCTIONS_WORKER_RUNTIME                  | The language worker runtime to load in the function app (**must be node**)                              |
| SQLDB_CONNECTION_STRING                   | [mssql node module](https://www.npmjs.com/package/mssql) connection string (see timeout note below)     |
| WEBSITE_NODE_DEFAULT_VERSION              | Default version of Node.js (**Microsoft Azure default is recommended**)                                 |
| FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA | Staging schema name                                                                                     |
| FORECAST_LOCATION_URL                     | URL used to provide the forecast location data                                                          |
| FLUVIAL_DISPLAY_GROUP_WORKFLOW_URL        | URL used to provide the fluvial display groups workflow reference data                                  |
| NON_DISPLAY_GROUP_WORKFLOW_URL            | URL used to provide the non display groups workflow reference data                                      |
| IGNORED_WORKFLOW_URL                      | URL used to provide the ignored workflows                                                               |

### Request Timeout Considerations

Successful loading of core forecasting engine data into the staging database requires a compatible database request timeout larger than the
default of 15 seconds. Request timeout tuning is achieved through the mssql connection string (please see the
[mssql node module](https://www.npmjs.com/package/mssql) documentation) specified in the SQLDB_CONNECTION_STRING application setting.

## Mandatory Runtime Function App Settings/Environment Variables If Using Microsoft Azure Service Bus Topics

| name                                                  | description                                                                                    |
|-------------------------------------------------------|------------------------------------------------------------------------------------------------|
| AZURE_SERVICE_BUS_EVENT_CODE_SUBSCRIPTION_NAME        | Subscription name associated with fews-eventcode-topic                                         |
| AZURE_SERVICE_BUS_STAGED_TIMESERIES_SUBSCRIPTION_NAME | Subscription name associated with fews-staged-timeseries-topic                                 |
| AZURE_SERVICE_BUS_FLUVIAL_DISPLAY_GROUP_SUBSCRIPTION_NAME | Subscription name associated with fews-display-group-topic                                 |
| AZURE_SERVICE_BUS_NON_DISPLAY_GROUP_SUBSCRIPTION_NAME | Subscription name associated with fews-non-display-group-topic (no fluvial/coastal distinction)|
| AZURE_SERVICE_BUS_FORECAST_LOCATION_SUBSCRIPTION_NAME | Subscription name associated with fews-forecast-location-topic                                 |
| AZURE_SERVICE_BUS_IGNORED_WORKFLOWS_SUBSCRIPTION_NAME | Subscription name associated with fews-ignored-workflows-topic                                 |

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

## Optional Runtime Function App Settings/Environment Variables

| name                         | description                                                                                                             |
|------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| SQLDB_LOCK_TIMEOUT           | Time limit for database lock acquisition in milliseconds (defaults to 6500ms)                                           |
| FEWS_START_TIME_OFFSET_HOURS | Number of hours before task completion time that core forecasting engine data should be retrieved for (defaults to 12\)  |
| FEWS_END_TIME_OFFSET_HOURS   | Number of hours after task completion time that core forecasting engine data should be retrieved for (defaults to 120)  |
