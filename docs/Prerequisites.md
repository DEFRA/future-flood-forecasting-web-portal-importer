
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
  * The URL for retrieving coastal MVT forecast location data.
  * The URL for retrieving coastal Tidal forecast location data.
  * The URL for retrieving coastal Triton forecast location data.
  * The URL for retrieving the set of fluvial locations associated with each core forecasting engine display group.
  * The URL for retrieving the set of coastal locations associated with each core forecasting engine display group.
  * The URL for retrieving the set of core forecasting engine filters associated with each workflow.
  * The URL for retrieving the set of ignored workflows.
  * The URL for retrieving the set of multivariate thresholds data.

### Microsoft Azure Service Bus Queues

* Microsoft Azure service bus queue named **fews-eventcode-queue**  
  * Messages are placed on this queue when a task run has completed within the core forecasting engine. Messages placed on this queue provide information on the completed task run to be processed by the **ProcessFewsEventCode** function.  The **ProcessFewsEventCode** function places a message for each
  plot and filter associated with the task run on a Microsoft Azure service bus queue named **fews-import-queue**.
  * Following an outage, the **ReplayProcessFewsEventCode** function can be enabled temporarily to allow messages on the **replay-fews-eventcode-dead-letter-queue** to be placed back on **fews-eventcode-queue** (see [Replaying Messages From Dead Letter Queues After An Outage](./Replaying-dead-letter-messages.md))
* Microsoft Azure service bus queue named **fews-import-queue**.
  * A message is placed on this queue for each plot and filter associated with a completed task run within the core forecasting engine. Messages are
  processed by the **ImportFromFews** function. Message processing extracts timeseries associated with a plot or filter from the core forecasting engine and loads the data into the staging database.
  * Following an outage, the **ReplayImportFromFews** function can be enabled temporarily to allow messages on **replay-fews-import-dead-letter-queue** to be placed back on **fews-import-queue** (see [Replaying Messages From Dead Letter Queues After An Outage](./Replaying-dead-letter-messages.md))
* Microsoft Azure service bus queue named **fews-fluvial-forecast-location-queue**  
  * Messages are placed on this queue when the set of fluvial forecast locations is updated. Messages are processed by the **RefreshFluvialForecastLocationData** function. Message processing retrieves the updated data and uses it to replace the content of the **FLUVIAL_FORECAST_LOCATION** table.
* Microsoft Azure service bus queue named **fews-coastal-tidal-forecast-location-queue**  
  * Messages are placed on this queue when the set of coastal forecast locations is updated. Messages are processed by the **RefreshCoastalTidalForecastLocationData** function. Message processing retrieves the updated data and uses it to replace the content of the **COASTAL_FORECAST_LOCATION** table.
* Microsoft Azure service bus queue named **fews-coastal-triton-forecast-location-queue**  
  * Messages are placed on this queue when the set of coastal forecast locations is updated. Messages are processed by the **RefreshCoastalTritonForecastLocationData** function. Message processing retrieves the updated data and uses it to replace the content of the **COASTAL_FORECAST_LOCATION** table.
* Microsoft Azure service bus queue named **fews-coastal-mvt-forecast-location-queue**  
  * Messages are placed on this queue when the set of coastal forecast locations is updated. Messages are processed by the **RefreshCoastalMVTForecastLocationData** function. Message processing retrieves the updated data and uses it to replace the content of the **COASTAL_FORECAST_LOCATION** table.
* Microsoft Azure service bus queue named **fews-fluvial-display-group-queue**
  * Messages are placed on this queue when the set of core forecasting engine workflows associated with fluvial forecast data is updated. Messages are processed by the **RefreshFluvialDisplayGroupData** function. Message processing retrieves the updated data and uses it to replace the content of the **FLUVIAL_DISPLAY_GROUP_WORKFLOW** table.
* Microsoft Azure service bus queue named **fews-coastal-display-group-queue**  
  * Messages are placed on this queue when the set of core forecasting engine workflows associated with coastal forecast data is updated. Messages are processed by the **RefreshCoastalDisplayGroupData** function. Message processing retrieves the updated data and uses it to replace the content of the **COASTAL_DISPLAY_GROUP_WORKFLOW** table.
* Microsoft Azure service bus queue named **fews-non-display-group-queue**  
  * Messages are placed on this queue when the set of core forecasting engine workflows associated with non-forecast data is updated. Messages are processed by the **RefreshNonDisplayGroupData** function. Message processing retrieves the updated data and uses it to replace the content of the **NON_DISPLAY_GROUP_WORKFLOW** table.
* Microsoft Azure service bus queue named **fews-ignored-workflows-queue**  
  * Messages are placed on this queue when the set of core forecasting engine workflows that should be ignored for staging purposes is updated . Messages are processed by the **RefreshIgnoredWorkflowData** function. Message processing retrieves the updated data and uses it to replace the content of the **IGNORED_WORKFLOW** table.
* Microsoft Azure service bus queue named **fews-mvt-queue**  
  * Messages are placed on this queue when the set of multivariate thresholds mapping data is updated. Messages are processed by the **RefreshMVTData** function. Message processing retrieves the updated data and uses it to replace the content of the **MULTIVARIATE_THRESHOLDS** table.
* Microsoft Azure service bus queue named **replay-fews-eventcode-dead-letter-queue**
  * Messages are forwarded to this queue from the **fews-eventcode-queue/$DeadLetterQueue**. Messages placed on this queue are processed by the **ReplayProcessFewsEventCode** function; this function places each message back onto the **fews-eventcode-queue** for message replay. This additional named replay queue is used instead of **fews-eventcode-queue/$DeadLetterQueue** because it can be configured to remove expired messages (a dead letter queue cannot).
* Microsoft Azure service bus queue named **replay-fews-import-dead-letter-queue**
  * Messages are forwarded to this queue from the **fews-import-queue/$DeadLetterQueue**. Messages placed on this queue are processed by the **ReplayImportFromFews** function; this function places each message back onto the **fews-eventcode-queue** for message replay. This additional named replay queue is used instead of **fews-import-queue/$DeadLetterQueue** because it can be configured to remove expired messages (a dead letter queue cannot).
* Optional Microsoft Azure service bus queue named **fews-staged-timeseries-queue**  
  * This queue is optional and only required when combined with a corresponding active output binding on the **ImportFromFews** function. The output binding is activated by setting the optional build time environment variable **IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED** to a value of **true** (see [Non-test related function app settings and environment variables](./Non-test-settings-and-environment-variables.md)). Messages are placed on this queue when the **ImportFromFews** function loads timeseries data associated with a task run into the staging database. A message is sent for each row inserted into the **TIMESERIES** table.

## Redundant Legacy Prerequisites

The function app prerequisites below are no longer required. It is recommended that they should be removed from any existing installation
accordingly.

**This list reflects a decision to remove support for Microsoft Azure service bus topics.**

* Microsoft Azure storage queue named **fewspiqueue**
* Microsoft Azure service bus queue named **fews-location-lookup-queue**
* Microsoft Azure service bus topic named **fews-location-lookup-topic** and associated topic subscription
* Microsoft Azure service bus queue named **fews-display-group-queue**
* Microsoft Azure service bus topic named **fews-display-group-topic** and associated topic subscription
* Microsoft Azure service bus queue named **fews-fluvial-non-display-group-queue**
* Microsoft Azure service bus topic named **fews-fluvial-non-display-group-topic** and associated topic subscription
* Microsoft Azure service bus queue named **fews-coastal-non-display-group-queue**
* Microsoft Azure service bus topic named **fews-coastal-non-display-group-topic** and associated topic subscription
* Microsoft Azure service bus queue named **fews-forecast-location-queue**
* Microsoft Azure service bus topic named **fews-forecast-location-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-eventcode-topic** and associated topic subscription
* Microsoft Azure service bus queue named **fews-import-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-fluvial-forecast-location-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-coastal-tidal-forecast-location-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-coastal-triton-forecast-location-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-coastal-mvt-forecast-location-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-fluvial-display-group-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-coastal-display-group-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-non-display-group-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-ignored-workflows-topic** and associated topic subscription
* Microsoft Azure service bus topic named **fews-mvt-topic** and associated topic subscription
* Optional Microsoft Azure service bus topic named **fews-staged-timeseries-topic** and associated topic subscription. This topic is optional and only required when combined with a corresponding active output binding on the **ImportFromFews** function.
