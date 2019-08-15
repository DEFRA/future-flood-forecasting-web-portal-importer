#!/bin/bash

if  [[ -d testing/staging-database/future-flood-forecasting-web-portal-staging ]]; then
  cd testing/staging-database/future-flood-forecasting-web-portal-staging
  docker-compose down --rmi all
  cd ..
  rm -rf future-flood-forecasting-web-portal-staging
  echo "******** Unit test staging database has been destroyed "
fi


