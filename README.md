# Future Flood Forecasting Web Portal Importer

Node.js Microsoft Azure functions responsible for extracting data from the core forecasting engine and importing it into a staging database prior to transformation for reporting and visualisation purposes.

* Message based triggering is used when:
  * Importing data for frequently updated locations that are not associated with a core forecasting engine display group.
  * Importing data for multiple locations associated with a core forecasting engine display group.
  * Refreshing the list of fluvial forecast locations.
  * Refreshing the set of fluvial locations associated with each core forecasting engine display group.
  * Refreshing the set of core forecasting engine filters associated with each workflow.
  * Refreshing the set of core forecasting engine ignored workflows.
* Messages containing the primary keys of staging database records holding data extracted from the core forecasting engine
  are used to trigger reporting and visualisation activities.  

## Contents

* [Prerequisites](README/Prerequisites.md)
* [Installation activities](README/Installation-acitivities.md)
* [Funcation app settings and environment variables](README/Settings-and-environment.md)
* [Running the function](README/Running-the-function.md)
* [Testing](README/Testing.md)

## Contributing to this project

If you have an idea you'd like to contribute please log an issue.

All contributions should be submitted via a pull request.

## License

THIS INFORMATION IS LICENSED UNDER THE CONDITIONS OF THE OPEN GOVERNMENT LICENCE found at:

[http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3)

The following attribution statement MUST be cited in your products and applications when using this information.
