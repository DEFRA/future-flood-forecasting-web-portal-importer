#!/bin/bash

if  ! `nc -z $SQLTESTDB_HOST $SQLTESTDB_PORT`; then
  cd testing/staging-database
  rm -rf future-flood-forecasting-web-portal-staging 
  git clone https://github.com/DEFRA/future-flood-forecasting-web-portal-staging.git
  cd future-flood-forecasting-web-portal-staging
  git checkout feature/non-display-group-forecasts
  ./local-bootstrap.sh
fi

echo "******** Unit test staging database has started"
