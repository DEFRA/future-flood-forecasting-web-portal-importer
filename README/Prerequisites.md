
# Prerequisites

## Build Prerequisites

* Java 8 or above
* Maven 3.x
* A UNIX based operating system with bash installed

## Runtime Prerequisites

* Microsoft Azure resource group
* Microsoft Azure service bus
* Microsoft Azure storage account
* **JavaScript** Microsoft Azure function app with an **application service plan**
* Microsoft Azure SQL database configured using the [Future Flood Forecasting Web Portal Staging](https://github.com/DEFRA/future-flood-forecasting-web-portal-staging) project.
  * The function app must have connectivity to the Azure SQL database either through the use of a Microsoft Azure virtual network or
    appropriate firewall rules.
* The function app must have connectivity to the following locations (identified by environment variables below):
  * The URL for the core forecasting engine REST API.
  * The URL for retrieving fluvial forecast location data.
  * The URL for retrieving the set of fluvial locations associated with each core forecasting engine display group.
  * The URL for retrieving the set of core forecasting engine filters associated with each workflow.
  * The URL for retrieving the set of ignored workflows.

### Runtime Prerequisites When Using Microsoft Azure Service Bus Queues

* **Microsoft Azure service bus queue named _fews-eventcode-queue_**  
Messages are placed on this queue when a taskrun has completed within the third-party core engine. The messages placed on this queue provide information on the completed workflow to be processed by the _ImportTimeSeriesRouter_ function, this information is used to inform a timeseries retrieval query to the PI server API.
* **Microsoft Azure service bus queue named _fews-staged-timeseries-queue_**  
Messages are placed on this queue when the function app has finished a timeseries data load into the staging database. The messages placed on this queue are issued by the _ImportTimeSeriesRouter_ function, this queue  is used to inform the consumer application of a staging database update. A new message is created for each row inserted into the _TIMESERIES_ table.
* **Microsoft Azure service bus queue named _fews-forecast-location-queue_**  
Messages are placed on this queue when there has been an update to the remote forecast location reference data file, this file provides greater detail on each forecast location. The _RefreshForecastLocationData_ function is triggered to load the new file and provide up-to-date reference data in the _FORECAST_LOCATION_ table of the staging database.
* **Microsoft Azure service bus queue named _fews-fluvial-display-group-queue_**
Messages are placed on this queue when there has been an update to the remote fluvial display groups workflow reference data file, this file provides display group data (plotid & locationid) associated with each workflow listed. The _RefreshDisplayGroupData_ function (to be renamed _FluvialRefreshDisplayGroupData_) is triggered to load the new file and provide up-to-date reference data in the _FLUVIAL_DISPLAY_GROUP_WORKFLOW_ table of the staging database.
* **Microsoft Azure service bus queue named _fews-fluvial-non-display-group-queue_**  
Messages are placed on this queue when there has been an update to the remote fluvial non display groups workflow reference data file, this file provides non-display group data (filterid & locationid) associated with each workflow listed. The _RefreshNonDisplayGroupData_ function (to be renamed _FluvialRefreshNonDisplayGroupData_) is triggered to load the new file and provide up-to-date reference data in the _FLUVIAL_NON_DISPLAY_GROUP_WORKFLOW_ table of the staging database.
* **Microsoft Azure service bus queue named _fews-coastal-display-group-queue_**  
Messages are placed on this queue when there has been an update to the remote coastal display groups workflow reference data file, this file provides display group data (plotid & locationid) associated with each workflow listed. The _CoastalRefreshDisplayGroupData_ function (not yet created) is triggered to load the new file and provide up-to-date reference data in the _TBC_ table of the staging database.
* **Microsoft Azure service bus queue named _fews-coastal-non-display-group-queue_**  
Messages are placed on this queue when there has been an update to the remote coastal non display groups workflow reference data file, this file provides non-display group data (filterid & locationid) associated with each workflow listed. The _CoastalRefreshNonDisplayGroupData_ function (not yet created) is triggered to load the new file and provide up-to-date reference data in the _TBC_ table of the staging database.
* **Microsoft Azure service bus queue named _fews-ignored-workflows-queue_**  
Messages are placed on this queue when there has been an update to the remote ignored workflows reference data file, this file provides a list of workflows that should be ignored by the web portal. When a new message arrives on the queue, the _RefreshIgnoredWorkflowData_ function is triggered to load the new file and provide up-to-date reference data in the _IGNORED_WORKFLOW_ table of the staging database.

### Runtime Prerequisites When Using Microsoft Azure Service Bus Topics

* Microsoft Azure service bus topic named **fews-eventcode-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-staged-timeseries-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-forecast-location-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-fluvial-display-group-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-fluvial-non-display-group-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-coastal-display-group-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-coastal-non-display-group-topic** and associated topic subscription  
* Microsoft Azure service bus topic named **fews-ignored-workflows-topic** and associated topic subscription  
An input binding for the RefreshIgnoredWorkflowData function.

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
