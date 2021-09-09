#!/bin/bash

if  ! `nc -z $SQLTESTDB_HOST $SQLTESTDB_PORT`; then
  cd testing/staging-database
  rm -rf future-flood-forecasting-web-portal-staging
  git clone -b fix/make-datum-column-nullable https://github.com/DEFRA/future-flood-forecasting-web-portal-staging.git
  cd future-flood-forecasting-web-portal-staging
  ./local-bootstrap.sh
fi

echo "******** Unit test staging database has started"
