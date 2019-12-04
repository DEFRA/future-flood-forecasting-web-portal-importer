#!/bin/bash

# Use the exit code from Jest as the script exit code so that test
# failures are propagated to any continuous integration/deployment
# pipeline. The unit test database should be destroyed irrespective
# of the success or failure of unit tests.
if [ -d "testing/coverage" ]; then
rm -r testing/coverage
echo -e "******* The most recent unit test 'coverage' folder has been removed.\n*\n******* Running a new unit test suite."
fi
npm run lint && testing/staging-database/bootstrap.sh && jest --config jestconfig.json
exitCode=$?
testing/staging-database/cleanup.sh
exit ${exitCode}
