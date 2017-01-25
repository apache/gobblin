#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Test script used by Travis to test the
# hadoop1 or hadoop2 versions of gobblin.
#

#!/bin/bash
set -e

#free

RUN_TEST_GROUP=${RUN_TEST_GROUP:-default}

script_dir=$(dirname $0)
echo "Old GRADLE_OPTS=$GRADLE_OPTS"

export java_version=$(java -version 2>&1 | grep 'java version' | sed -e 's/java version "\(1\..\).*/\1/')

echo "Using Java version:${java_version}"

export GOBBLIN_GRADLE_OPTS="-Dorg.gradle.daemon=false -Dgobblin.metastore.testing.embeddedMysqlEnabled=false -PusePreinstalledMysql=true -PjdkVersion=${java_version}"

TEST_SCRIPT=${script_dir}/test-${RUN_TEST_GROUP}.sh
if [ -x $TEST_SCRIPT ] ; then
  echo "Running test group $RUN_TEST_GROUP"
  $TEST_SCRIPT "$@"
else
  echo "Test file $TEST_SCRIPT does not exist or is not executable!"
  exit 1
fi
