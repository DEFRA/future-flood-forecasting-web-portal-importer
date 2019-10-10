#!/bin/bash

mvn clean -f build/pom.xml process-resources
cp build/target/host.json.template host.json
cp build/src/main/resources/functions/ImportTimeSeriesDisplayGroups/$BUILD_TYPE/function.json ImportTimeSeriesDisplayGroups/
cp build/src/main/resources/functions/RefreshLocationLookupData/$BUILD_TYPE/function.json RefreshLocationLookupData/