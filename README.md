# Future Flood Forecasting Web Portal Importer

A Node.js Microsoft Azure function responsible for extracting data from the core forecasting engine and importing it into a staging database prior to
transformation for reporting and visualisation purposes. Queue storage based triggering is used.

## Prerequisites

### Mandatory

* Microsoft Azure resource group
* Microsoft Azure storage account
* Microsoft Azure storage queue named **fewspiqueue**
* **Node.js** Microsoft Azure function app with an **application service plan**
* Microsoft Azure SQL database configured using the [Future Flood Forecasting Web Portal Staging](https://github.com/DEFRA/future-flood-forecasting-web-portal-staging) project.
  * The function app must have connectivity to the Azure SQL database either through the use of a Microsoft Azure virtual network or
    appropriate firewall rules.

## Function App Settings/Environment Variables

| name                            | description                                                                                             |
|---------------------------------|---------------------------------------------------------------------------------------------------------|
| APPINSIGHTS_INSTRUMENTATIONKEY  | Instrumention key controlling if telemetry is sent to the ApplicationInsights service                   |
| AzureWebJobsStorage             | Storage account connection string used by the function app                                              |
| AZURE_STORAGE_CONNECTION_STRING | Storage account connection string used by the function app                                              |
| FEWS_PI_API                     | Protocol, fully qualified domain name and optional port of the core forecasting engine REST API         |
| FUNCTIONS_EXTENSION_VERSION     | Functions runtime version (**must be ~2**)                                                              |
| FUNCTIONS_WORKER_RUNTIME        | The language worker runtime to load in the function app (**must be node**)                              |
| SQLDB_CONNECTION_STRING         | [mssql node module](https://www.npmjs.com/package/mssql) connection string                              |
| WEBSITE_NODE_DEFAULT_VERSION    | Default version of Node.js (**Microsoft Azure default is recommended**)                                 |

## Installation Activities

The following activities need to be performed for the function to run. While the documentation states what activities need to be performed it
does not prescribe how the activities should be performed.

* Configure app settings/environment variables
* Install node modules
* Install function extensions
* Deploy the function to the function app

## Running The Function

Messages placed on the storage queue **must** contain only the ID of the location for which data is to be imported.

## Contributing to this project

If you have an idea you'd like to contribute please log an issue.

All contributions should be submitted via a pull request.

## License

THIS INFORMATION IS LICENSED UNDER THE CONDITIONS OF THE OPEN GOVERNMENT LICENCE found at:

[http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3)

The following attribution statement MUST be cited in your products and applications when using this information.
