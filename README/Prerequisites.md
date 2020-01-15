
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

* Microsoft Azure service bus queue named **fews-eventcode-queue**  
An input binding for the 'ImportTimseseriesRouter' function.
* Microsoft Azure service bus queue named **fews-staged-timeseries-queue**  
An output binding for the 'ImportTimseseriesRouter' function.
* Microsoft Azure service bus queue named **fews-forecast-location-queue**  
An input binding for the 'RefreshForecastLocationData' function.
* Microsoft Azure service bus queue named **fews-fluvial-display-group-queue**  
An input binding for the 'RefreshDisplayGroupData' function (to be renamed *FluvialRefreshDisplayGroupData*).
* Microsoft Azure service bus queue named **fews-fluvial-non-display-group-queue**  
An input binding for the 'RefreshNonDisplayGroupData' function (to be renamed *FluvialRefreshNonDisplayGroupData*).
* Microsoft Azure service bus queue named **fews-coastal-display-group-queue**  
An input binding for the 'CoastalRefreshDisplayGroupData' function (The *CoastalRefreshDisplayGroupData* function has not yet been created).
* Microsoft Azure service bus queue named **fews-coastal-non-display-group-queue**  
An input binding for the 'CoastalRefreshNonDisplayGroupData' function (The *CoastalRefreshNonDisplayGroupData* function has not yet been created).
* Microsoft Azure service bus queue named **fews-ignored-workflows-queue**  
An input binding for the 'RefreshIgnoredWorkflowData' function.

### Runtime Prerequisites When Using Microsoft Azure Service Bus Topics

* Microsoft Azure service bus topic named **fews-eventcode-topic** and associated topic subscription  
An input binding for the 'ImportTimseseriesRouter' function.
* Microsoft Azure service bus topic named **fews-staged-timeseries-topic** and associated topic subscription  
An output binding for the 'ImportTimseseriesRouter' function.
* Microsoft Azure service bus topic named **fews-forecast-location-topic** and associated topic subscription  
An input binding for the 'RefreshForecastLocationData' function.
* Microsoft Azure service bus topic named **fews-fluvial-display-group-topic** and associated topic subscription  
An input binding for the 'RefreshDisplayGroupData' function (to be renamed *FluvialRefreshDisplayGroupData*).
* Microsoft Azure service bus topic named **fews-fluvial-non-display-group-topic** and associated topic subscription  
An input binding for the 'RefreshNonDisplayGroupData' function (to be renamed *FluvialRefreshNonDisplayGroupData*).  
* Microsoft Azure service bus topic named **fews-coastal-display-group-topic** and associated topic subscription  
An input binding for the 'CoastalRefreshDisplayGroupData' function (The *CoastalRefreshDisplayGroupData* function has not yet been created).
* Microsoft Azure service bus topic named **fews-coastal-non-display-group-topic** and associated topic subscription  
An input binding for the 'CoastalRefreshNonDisplayGroupData' function (The *CoastalRefreshNonDisplayGroupData* function has not yet been created).
* Microsoft Azure service bus topic named **fews-ignored-workflows-topic** and associated topic subscription  
An input binding for the 'RefreshIgnoredWorkflowData' function.

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
