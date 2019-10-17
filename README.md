# Future Flood Forecasting Web Portal Importer

Node.js Microsoft Azure functions responsible for extracting data from the core forecasting engine and importing it into a staging database prior to transformation for reporting and visualisation purposes.

* Queue based triggering is used when:
  * Importing data for a single location during the previous twenty fours hours.
  * Importing data for multiple locations associated with a display group
  * Refreshing the list of forecast locations
  * Refreshing the set of locations associated with each display group.

## Prerequisites

### Mandatory

* Microsoft Azure resource group
* Microsoft Azure storage account
* Microsoft Azure storage queue named **fewspiqueue**
* Microsoft Azure storage queue named **fews-forecast-location-queue**
* Microsoft Azure storage queue named **fews-location-lookup-queue**
* Microsoft Azure service bus
* Microsoft Azure service bus queue or storage queue named **fews-eventcode-queue**
* Microsoft Azure service bus topic named **fews-eventcode-topic**
* **Node.js** Microsoft Azure function app with an **application service plan**
* Microsoft Azure SQL database configured using the [Future Flood Forecasting Web Portal Staging](https://github.com/DEFRA/future-flood-forecasting-web-portal-staging) project.
  * The function app must have connectivity to the Azure SQL database either through the use of a Microsoft Azure virtual network or
    appropriate firewall rules.
* A UNIX based operating system with bash and the nc utility installed is required to run unit tests.
  * If using Microsoft Windows, you may wish to consider using the [Windows Subsystem For Linux](https://docs.microsoft.com/en-us/windows/wsl/about).

## Unit Testing Considerations

As this Azure function app is responsible for placing data extracted from the core forecasting engine into an Azure SQL database, unit tests
need to check that the database is populated correctly. As such, rather than mocking database functionality, a dedicated database instance is required for unit testing purposes. This dedicated database instance must be created in the same way as non-unit test specific instances using the [Future Flood Forecasting Web Portal Staging](https://github.com/DEFRA/future-flood-forecasting-web-portal-staging) project. Unit test specific environment variables (defined below) must be set to allow the unit tests to utilise a dedicated database instance.

* If unit test specific environment variables identify an existing database instance, the instance will be used by unit tests.
* If unit test specific environment variables do not identify an existing database instance a docker based Microsoft SQL Server instance will be
  created for use by the unit tests.
  * The creation of docker based Microsoft SQL Server instances relies on the prerequisites of the [Future Flood Forecasting Web Portal Staging](https://github.com/DEFRA/future-flood-forecasting-web-portal-staging) project.

## Function App Settings/Environment Variables

| name                                      | description                                                                                             |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------|
| APPINSIGHTS_INSTRUMENTATIONKEY            | Instrumention key controlling if telemetry is sent to the ApplicationInsights service                   |
| AzureWebJobsStorage                       | Storage account connection string used by the function app                                              |
| AZURE_STORAGE_CONNECTION_STRING           | Storage account connection string used by the function app                                              |
| FEWS_PI_API                               | Protocol, fully qualified domain name and optional port of the core forecasting engine REST API         |
| FUNCTIONS_EXTENSION_VERSION               | Functions runtime version (**must be ~2**)                                                              |
| FUNCTIONS_WORKER_RUNTIME                  | The language worker runtime to load in the function app (**must be node**)                              |
| SQLDB_CONNECTION_STRING                   | [mssql node module](https://www.npmjs.com/package/mssql) connection string                              |
| WEBSITE_NODE_DEFAULT_VERSION              | Default version of Node.js (**Microsoft Azure default is recommended**)                                 |
| FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA | Staging schema name                                                                                     |
| FEWS_LOCATION_IDS                         | Semi-colon separated list of locations used with scheduled imports                                      |
| FEWS_PLOT_ID                              | The core forecasting engine plot ID used with scheduled imports                                         |
| FORECAST_LOCATION_URL                     | URL used to provide the forecast location data                                                          |
| LOCATION_LOOKUP_URL                       | URL used to provide location lookup data associated with display groups                                 |

### Redundant Legacy Function App Settings/Environment Variables

The function app settings/environment variables below are no longer used. It is recommended that they should be removed from any existing installation
accordingly.

| name                                      | description                                                                                             |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------|
| FEWS_INITIAL_LOAD_HISTORY_HOURS           | Number of hours before the initial import time that core forecasting engine data should be retrieved for|
| FEWS_LOAD_HISTORY_HOURS                   | Number of hours before subsequent import times that core forecasting engine data should be retrieved for|
| FEWS_IMPORT_DISPLAY_GROUPS_SCHEDULE       | UNIX Cron expression controlling when time series display groups are imported                           |

### Optional Function App Settings/Environment Variables

| name                         | description                                                                                                          |
|------------------------------|----------------------------------------------------------------------------------------------------------------------|
| SQLDB_LOCK_TIMEOUT           | Time limit for database lock acquisition in milliseconds (defaults to 6500ms)                                        |
| FEWS_START_TIME_OFFSET_HOURS | Number of hours before the current time that core forecasting engine data should be retrieved for (defaults to 48)   |
| FEWS_END_TIME_OFFSET_HOURS   | Number of hours after the current time that core forecasting engine data should be retrieved for (defaults to 120)   |

### Unit Test Specific Environment Variables

| name                                      | description                                                                                             |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------|
| SQLTESTDB_HOST                            | Database host used for unit tests                                                                       |
| SQLTESTDB_PORT                            | Database port used for unit tests                                                                       |
| SQLTESTDB_REQUEST_TIMEOUT                 | The database request timeout for unit tests (in milliseconds) - defaults to 15000ms                     |
| TEST_TIMEOUT                              | Optional Unit test timeout override (in milliseconds) - defaults to 5000ms                              |

## Installation Activities

The following activities need to be performed for the function to run. While the documentation states what activities need to be performed it
does not prescribe how the activities should be performed.

* Configure app settings/environment variables
* Install node modules
* Install function extensions
* Deploy the functions to the function app

## Running The Queue Based Functions

* Messages placed on the fewspiqueue **must** contain only the ID of the location for which data is to be imported.
* Messages placed on the fews-location-lookup-queue **and** fews-forecast-location-queue **must** contain some content; for example {"input": "refresh"}.
  The message content is ignored.
* Messages placed on the fews-eventcode-queue or fews-eventcode-topic **must** adhere to the format used for
  Azure service bus alerts in the core forecasting engine.

## Contributing to this project

If you have an idea you'd like to contribute please log an issue.

All contributions should be submitted via a pull request.

## License

THIS INFORMATION IS LICENSED UNDER THE CONDITIONS OF THE OPEN GOVERNMENT LICENCE found at:

[http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3)

The following attribution statement MUST be cited in your products and applications when using this information.
