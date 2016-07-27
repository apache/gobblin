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

script_dir=$(dirname $0)

source ${script_dir}/test-groups.inc

echo "Starting $0 at " $(date)
echo "Precompiling tests"
./gradlew --debug compileTest 2>&1 | grep aws-java-sdk-1.7.4
echo "================================================"
find $HOME/.gradle/caches/ -name "aws-java-sdk-1.7.4*" | xargs ls -l
find $HOME/.gradle/caches/ -name "aws-java-sdk-1.7.4.jar" | xargs -n 1 hexdump -C | tail -300
echo "================================================"
echo "Running tests for $TEST_GROUP1"
time ./gradlew test -PskipTestGroup=disabledOnTravis -PrunTestGroups=$TEST_GROUP1 -Dorg.gradle.parallel=false
