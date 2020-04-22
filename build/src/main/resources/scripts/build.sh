#!/bin/bash

# Copy the configuration file for each function into place based on whether a queue or topic build is being performed. 
rm -f ImportTimeseriesRouter/function.json
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
cp build/src/main/resources/functions/ImportTimeseriesRouter/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json ImportTimeseriesRouter/
cp build/src/main/resources/functions/RefreshFluvialDisplayGroupData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshFluvialDisplayGroupData/
cp build/src/main/resources/functions/RefreshCoastalDisplayGroupData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshCoastalDisplayGroupData/
cp build/src/main/resources/functions/RefreshNonDisplayGroupData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshNonDisplayGroupData/
cp build/src/main/resources/functions/RefreshFluvialForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshFluvialForecastLocationData/
cp build/src/main/resources/functions/RefreshCoastalTidalForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshCoastalTidalForecastLocationData/
cp build/src/main/resources/functions/RefreshCoastalTritonForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshCoastalTritonForecastLocationData/
cp build/src/main/resources/functions/RefreshCoastalMVTForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshCoastalMVTForecastLocationData/
cp build/src/main/resources/functions/RefreshIgnoredWorkflowData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshIgnoredWorkflowData/
cp build/src/main/resources/functions/DeleteExpiredTimeseries/function.json DeleteExpiredTimeseries/