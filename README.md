# Incident Management Forecasting (Known Previously As Future Flood Forecasting) Web Portal Importer

[![CI](https://github.com/DEFRA/future-flood-forecasting-web-portal-importer/actions/workflows/ci.yml/badge.svg)](https://github.com/DEFRA/future-flood-forecasting-web-portal-importer/actions/workflows/ci.yml)
[![Test Coverage](https://api.codeclimate.com/v1/badges/1ac657643bee19c0a22a/test_coverage)](https://codeclimate.com/github/DEFRA/future-flood-forecasting-web-portal-importer/test_coverage)
[![Maintainability](https://api.codeclimate.com/v1/badges/1ac657643bee19c0a22a/maintainability)](https://codeclimate.com/github/DEFRA/future-flood-forecasting-web-portal-importer/maintainability)

Node.js Microsoft Azure functions responsible for extracting data from the [core forecasting engine](https://www.deltares.nl/en/software/flood-forecasting-system-delft-fews-2/) and importing it into a [staging database](https://github.com/DEFRA/future-flood-forecasting-web-portal-staging) prior to transformation for reporting and visualisation purposes external to the core forecasting engine. Data extraction is configuration based, message driven and achieved using a [REST API](https://publicwiki.deltares.nl/display/FEWSDOC/FEWS+PI+REST+Web+Service). Data extraction and import activites are performed in a fault tolerant manner to maxmise the amount of data available for reporting and visualisation purposes.

* Message based triggering is used when:
  * Importing data for frequently updated locations that are not associated with a core forecasting engine display group.
  * Importing data for multiple locations associated with a core forecasting engine fluvial display group.
  * Importing data for multiple locations associated with a core forecasting engine coastal display group.
  * Replaying data imports following an outage.
  * Refreshing the list of fluvial forecast locations.
  * Refreshing the list of coastal forecast locations. One message trigger for each of the three coastal location types.
  * Refreshing multivariate threshold data.
  * Refreshing the set of fluvial locations associated with each core forecasting engine display group.
  * Refreshing the set of coastal locations associated with each core forecasting engine display group.
  * Refreshing the set of core forecasting engine filters associated with each workflow.
  * Refreshing the set of core forecasting engine ignored workflows.
  * Sending a notification when an update of all configuration data is detected.
  * **Optional** - Messages containing the primary keys of staging database records holding data extracted from the core forecasting engine
  can be used to trigger reporting and visualisation activities (see [Prerequisites](docs/Prerequisites.md) and [Non-test related function app settings and environment variables](docs/Non-test-settings-and-environment-variables.md) for further details).
* CRON expression based triggering is used to periodically remove stale timeseries data from the staging database.

## Contents

* [Prerequisites](docs/Prerequisites.md)
* [Installation activities](docs/Installation-activities.md)
* [Non-test related function app settings and environment variables](docs/Non-test-settings-and-environment-variables.md)
* [Running the queue based functions](docs/Running-the-queue-functions.md)
* [Testing](docs/Testing.md)
* [Replaying Messages From Dead Letter Queues After An Outage](docs/Replaying-dead-letter-messages.md)

## Contributing to this project

If you have an idea you'd like to contribute please log an issue.

All contributions should be submitted via a pull request.

## License

THIS INFORMATION IS LICENSED UNDER THE CONDITIONS OF THE OPEN GOVERNMENT LICENCE found at:

[http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3)

The following attribution statement MUST be cited in your products and applications when using this information.
