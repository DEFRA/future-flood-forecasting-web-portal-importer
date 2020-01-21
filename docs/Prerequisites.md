
# Prerequisites

## Build Prerequisites

* Java 8 or above
* Maven 3.x
* A UNIX based operating system with bash installed

## Runtime Prerequisites

* Microsoft Azure resource group
* Microsoft Azure service bus
* Microsoft Azure storage account
* **Node.js** Microsoft Azure function app with an **application service plan**
* Microsoft Azure SQL database configured using the [Future Flood Forecasting Web Portal Staging](https://github.com/DEFRA/future-flood-forecasting-web-portal-staging) project.
  * The function app must have connectivity to the Azure SQL database either through the use of a Microsoft Azure virtual network or
    appropriate firewall rules.
* The function app must have connectivity to the following locations (identified in the [environment variables](Non-test-settings-and-environment-variables.md) document):
  * The URL for the core forecasting engine REST API.
  * The URL for retrieving fluvial forecast location data.
  * The URL for retrieving the set of fluvial locations associated with each core forecasting engine display group.
  * The URL for retrieving the set of core forecasting engine filters associated with each workflow.
  * The URL for retrieving the set of ignored workflows.

### Runtime Prerequisites When Using Microsoft Azure Service Bus Queues

* Microsoft Azure service bus queue named **fews-eventcode-queue**  
  * Messages are placed on this queue when a task run has completed within the core forecasting engine. Messages placed on this queue provide information on the completed task run to be processed by the **ImportTimeSeriesRouter** function.  The **ImportTimeSeriesRouter** function extracts timeseries from the core forecasting engine and loads the data into the staging database.
* Microsoft Azure service bus queue named **fews-staged-timeseries-queue**  
  * Messages are placed on this queue when the **ImportTimeSeriesRouter** function loads timeseries data associated with a task run into the staging database. A message is sent for each row inserted into the **TIMESERIES** table.
* Microsoft Azure service bus queue named **fews-fluvial-forecast-location-queue**  
  * Messages are placed on this queue when the set of fluvial forecast locations is updated. Messages are processed by the **RefreshForecastLocationData** function. Message processing retrieves the updated data and uses it to replace the content of the **FLUVIAL_FORECAST_LOCATION** table.
* Microsoft Azure service bus queue named **fews-fluvial-display-group-queue**
  * Messages are placed on this queue when the set of core forecasting engine workflows associated with fluvial forecast data is updated. Messages are processed by the **RefreshDisplayGroupData** function. Message processing retrieves the updated data and uses it to replace the content of the **FLUVIAL_DISPLAY_GROUP_WORKFLOW** table.
* Microsoft Azure service bus queue named **fews-fluvial-non-display-group-queue**  
  * Messages are placed on this queue when the set of core forecasting engine workflows associated with fluvial non-forecast data is updated. Messages are processed by the **RefreshNonDisplayGroupData** function. Message processing retrieves the updated data and uses it to replace the content of the **FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW** table.
* Microsoft Azure service bus queue named **fews-coastal-display-group-queue**  
  * Messages are placed on this queue when the set of core forecasting engine workflows associated with coastal forecast data is updated. Messages are processed by the **TBD** function. Message processing retrieves the updated data and uses it to replace the content of the **TBD** table.
* Microsoft Azure service bus queue named **fews-coastal-non-display-group-queue**  
  * Messages are placed on this queue when the set of core forecasting engine workflows associated with coastal non-forecast data is updated. Messages are processed by the **TBD** function. Message processing retrieves the updated data and uses it to replace the content of the **TBD** table.
* Microsoft Azure service bus queue named **fews-ignored-workflows-queue**  
  * Messages are placed on this queue when the set of core forecasting engine workflows that should be ignored for staging puposes is updated . Messages are processed by the **RefreshIgnoredWorkflowData** function. Message processing retrieves the updated data and uses it to replace the content of the **IGNORED_WORKFLOW** table.

### Runtime Prerequisites When Using Microsoft Azure Service Bus Topics

* Microsoft Azure service bus topic named **fews-eventcode-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-staged-timeseries-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-forecast-location-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-fluvial-display-group-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-fluvial-non-display-group-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-coastal-display-group-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-coastal-non-display-group-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-ignored-workflows-topic** and associated topic subscription

The purpose of each topic is analagous to that of each corresponding queue.

## Redundant Legacy Prerequisites

The function app prerequisites below are no longer required. It is recommended that they should be removed from any existing installation
accordingly.

* Microsoft Azure storage queue named **fewspiqueue**
* Microsoft Azure service bus queue named **fews-location-lookup-queue**
* Microsoft Azure service bus topic named **fews-location-lookup-topic** and associated topic subscription
* Microsoft Azure service bus queue named **fews-display-group-queue**
* Microsoft Azure service bus topic named **fews-display-group-topic** and associated topic subscription
* Microsoft Azure service bus queue named **fews-non-display-group-queue**
* Microsoft Azure service bus topic named **fews-non-display-group-topic** and associated topic subscription
