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

script_dir=$(dirname $0)

source ${script_dir}/test-groups.inc

echo "Starting $0 at " $(date)
echo "GOBBLIN_GRADLE_OPTS=$GOBBLIN_GRADLE_OPTS"
time ./gradlew -PskipTestGroup=disabledOnTravis,$TEST_GROUP1 -Dorg.gradle.parallel=false $GOBBLIN_GRADLE_OPTS test
