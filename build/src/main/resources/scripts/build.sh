#!/bin/bash

# Copy the configuration file for each function into place based on whether a queue or topic build is being performed. 
rm -f ImportTimeseriesRouter/function.json
rm -f RefreshDisplayGroupData/function.json
rm -f RefreshNonDisplayGroupData/function.json
rm -f RefreshForecastLocationData/function.json
rm -f RefreshIgnoredWorkflowData/function.json
mvn clean -f build/pom.xml process-resources
cp build/target/host.json.template host.json
cp build/src/main/resources/functions/ImportTimeseriesRouter/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json ImportTimeseriesRouter/
cp build/src/main/resources/functions/RefreshDisplayGroupData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshDisplayGroupData/
cp build/src/main/resources/functions/RefreshNonDisplayGroupData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshNonDisplayGroupData/
cp build/src/main/resources/functions/RefreshForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshForecastLocationData/
cp build/src/main/resources/functions/RefreshIgnoredWorkflowData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshIgnoredWorkflowData/