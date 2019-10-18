#!/bin/bash

# Copy the configuration file for each function into place based on whether a queue or topic build is being performed. 
rm -f ImportTimeSeriesDisplayGroups/function.json
rm -f RefreshLocationLookupData/function.json
rm -f RefreshForecastLocationData/function.json
mvn clean -f build/pom.xml process-resources
cp build/target/host.json.template host.json
cp build/src/main/resources/functions/ImportTimeSeriesDisplayGroups/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json ImportTimeSeriesDisplayGroups/
cp build/src/main/resources/functions/RefreshLocationLookupData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshLocationLookupData/
cp build/src/main/resources/functions/RefreshForecastLocationData/$FFFS_WEB_PORTAL_BUILD_TYPE/function.json RefreshForecastLocationData/