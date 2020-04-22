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
* Messages placed on the fews-eventcode-queue or fews-eventcode-topic **must** adhere to the format used for
  Azure service bus alerts in the core forecasting engine.
* Messages placed on the fews-staged-timeseries-queue or fews-staged-timeseries-topic **must** conform to the following format:
  * { id: "&lt;&lt;Primary key of the staging database record holding data obtained from the core forecasting engine&gt;&gt;" }
  