
# Unit Testing

## Operating System

* A UNIX based operating system with bash and the nc utility installed is required to run unit tests.
  * If using Microsoft Windows, you may wish to consider using the [Windows Subsystem For Linux](https://docs.microsoft.com/en-us/windows/wsl/about).

### Additional Considerations

As this Azure function app is responsible for placing data extracted from the core forecasting engine into an Azure SQL database, unit tests
need to check that the database is populated correctly. As such, rather than mocking database functionality, a dedicated database instance is required for unit testing purposes. This dedicated database instance must be created in the same way as non-unit test specific instances using the [Future Flood Forecasting Web Portal Staging](https://github.com/DEFRA/future-flood-forecasting-web-portal-staging) project. Unit test specific environment variables (defined below) must be set to allow the unit tests to utilise a dedicated database instance.

* If unit test specific environment variables identify an existing database instance, the instance will be used by unit tests.
* If unit test specific environment variables do not identify an existing database instance a docker based Microsoft SQL Server instance will be
  created for use by the unit tests.
  * The creation of docker based Microsoft SQL Server instances relies on the prerequisites of the [Future Flood Forecasting Web Portal Staging](https://github.com/DEFRA/future-flood-forecasting-web-portal-staging) project.
  
## Exploratory Testing

### Sending Messages To Azure Service Bus Queues/Topics

In the absence of other means to send messages to Azure Service Bus Queues/Topics such as [Service Bus Explorer](https://code.msdn.microsoft.com/windowsapps/Service-Bus-Explorer-f2abca5a), basic test clients are provided. Mandatory and test client
specific environment variables need to be set (see below) and then one of the following commands should be run from the root
directory of this project.

* node testing/service-bus/publish-to-queue.js
* node testing/service-bus/publish-to-topic.js

### Exploratory Test Client Specific Environment Variables

| name                                      | description                                                                                             |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------|
| AZURE_SERVICE_BUS_QUEUE                   | The Azure service bus queue to which test messages are sent                                             |
| AZURE_SERVICE_BUS_TOPIC                   | The Azure service bus topic to which test messages are sent                                             |
| AZURE_SERVICE_BUS_TEST_MESSAGE            | The test message                                                                                        |

## Unit Test Specific Environment Variables

| name                                      | description                                                                                             |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------|
| SQLTESTDB_HOST                            | Database host used for unit tests                                                                       |
| SQLTESTDB_PORT                            | Database port used for unit tests                                                                       |
| SQLTESTDB_REQUEST_TIMEOUT                 | The database request timeout for unit tests (in milliseconds) - defaults to 15000ms                     |
| TEST_TIMEOUT                              | Optional unit test timeout override (in milliseconds) - defaults to 5000ms                              |

### Unit Test Coverage

Unit test coverage is provided by Istanbul, a test coverage tool built into Jest. Unit test coverage config is handled directly by Jest in the jestconfig file. A LCOV report is created for a unit test script run, it is placed in the testing/coverage directory. LCOV is a graphical front-end for gcov. It collects gcov data for multiple source files and creates HTML pages containing the source code annotated with coverage information. LCOV supports statement, function and branch coverage measurement.
