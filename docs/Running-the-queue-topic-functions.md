# Running The Queue/Topic Based Functions

* Messages placed on the following queues **must** contain some content (for example {"input": "refresh"}). The message content is ignored:  
  * fews-fluvial-display-group-queue
  * fews-coastal-display-group-queue
  * fews-non-display-group-queue  
  * fews-fluvial-forecast-location-queue
  * fews-coastal-tidal-forecast-location-queue
  * fews-coastal-triton-forecast-location-queue
  * fews-coastal-mvt-forecast-location-queue
  * fews-ignored-workflows-queue
  * fews-mvt-queue
* Messages placed on the fews-eventcode-queue or fews-eventcode-topic **must** adhere to the format used for
  Azure service bus alerts in the core forecasting engine.
* Messages placed on the fews-import-queue must be in JSON format and conform to either of the following:
  * ```yaml
    {
      "taskRunId": "<<Core forecasting engine task run ID>>",
      "filterId": "<<Core forecasting engine filter ID>>"
    }
  * ```yaml
    {
      "taskRunId": "<<Core forecasting engine task run ID>>",
      "plotId": "<<Core forecasting engine plot ID>>"
    }
* If the output binding is implemented, messages placed on the fews-staged-timeseries-queue or fews-staged-timeseries-topic **must** conform to the following format:
  * ```yaml
    {
      "id": "<<Primary key of the staging database record holding data obtained from the core forecasting engine>>"
    }
  