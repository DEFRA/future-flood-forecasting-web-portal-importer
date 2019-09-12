#!/bin/bash

# Use the exit code from Jest as the script exit code so that test
# failures are propagated to any continuous integration/deployment
# pipeline. The unit test database should be destroyed irrespective
# of the success or failure of unit tests.
npm run lint && testing/staging-database/bootstrap.sh && jest
exitCode=$?
testing/staging-database/cleanup.sh
exit ${exitCode}
