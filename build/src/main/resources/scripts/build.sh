#!/bin/bash

# Copy the configuration file for each function into place based on whether a queue or topic build is being performed. 
rm -f ProcessFewsEventCode/function.json
rm -f ReplayProcessFewsEventCode/function.json
rm -f ImportFromFews/function.json
rm -f ReplayImportFromFews/function.json
rm -f RefreshFluvialDisplayGroupData/function.json
rm -f RefreshCoastalDisplayGroupData/function.json
rm -f RefreshNonDisplayGroupData/function.json
rm -f RefreshFluvialForecastLocationData/function.json
rm -f RefreshCoastalTidalLocationData/function.json
rm -f RefreshCoastalTritonLocationData/function.json
rm -f RefreshCoastalMVTLocationData/function.json
rm -f RefreshIgnoredWorkflowData/function.json
rm -f DeleteExpiredTimeseries/function.json
mvn clean -f build/pom.xml process-resources
cp build/target/host.json.template host.json
if [[ "${IMPORT_TIMESERIES_OUTPUT_BINDING_REQUIRED}" == "true" ]]; then
  cp build/src/main/resources/functions/ImportFromFews/OutputBinding/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json ImportFromFews/
else
  cp build/src/main/resources/functions/ImportFromFews/NoOutputBinding/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json ImportFromFews/
fi
cp build/src/main/resources/functions/ReplayImportFromFews/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json ReplayImportFromFews/
cp build/src/main/resources/functions/ProcessFewsEventCode/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json ProcessFewsEventCode/
cp build/src/main/resources/functions/ReplayProcessFewsEventCode/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json ReplayProcessFewsEventCode/
cp build/src/main/resources/functions/RefreshFluvialDisplayGroupData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshFluvialDisplayGroupData/
cp build/src/main/resources/functions/RefreshCoastalDisplayGroupData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshCoastalDisplayGroupData/
cp build/src/main/resources/functions/RefreshNonDisplayGroupData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshNonDisplayGroupData/
cp build/src/main/resources/functions/RefreshFluvialForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshFluvialForecastLocationData/
cp build/src/main/resources/functions/RefreshCoastalTidalForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshCoastalTidalForecastLocationData/
cp build/src/main/resources/functions/RefreshCoastalTritonForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshCoastalTritonForecastLocationData/
cp build/src/main/resources/functions/RefreshCoastalMVTForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshCoastalMVTForecastLocationData/
cp build/src/main/resources/functions/RefreshIgnoredWorkflowData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshIgnoredWorkflowData/
cp build/src/main/resources/functions/RefreshMVTData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshMVTData/
cp build/src/main/resources/functions/DeleteExpiredTimeseries/function.json DeleteExpiredTimeseries/