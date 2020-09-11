#!/bin/bash

if  ! `nc -z $SQLTESTDB_HOST $SQLTESTDB_PORT`; then
  cd testing/staging-database
  rm -rf future-flood-forecasting-web-portal-staging 
  cd future-flood-forecasting-web-portal-staging
  ./local-bootstrap.sh
fi

echo "******** Unit test staging database has started"
