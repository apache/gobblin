#
# Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy of the
# License at  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied.
#

#
# Test script used by Travis to test the
# hadoop1 or hadoop2 versions of gobblin.
#

#!/bin/bash
set -e

RUN_TEST_GROUP=${RUN_TEST_GROUP:-default}

script_dir=$(dirname $0)

TEST_SCRIPT=${script_dir}/test-${RUN_TEST_GROUP}.sh
if [ -x $TEST_SCRIPT ] ; then
  echo "Running test group $RUN_TEST_GROUP"
  $TEST_SCRIPT "$@"
else
  echo "Test file $TEST_SCRIPT does not exist or is not executable!"
  exit 1
fi
