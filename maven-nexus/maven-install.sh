#!/bin/bash

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

#group is overiden to support forked repositories

function print_usage(){
  echo "maven-install.sh --version VERSION [--group GROUP]"
}

for i in "$@"
do
  case "$1" in
    --version)
      VERSION="$2"
      shift
      ;;
    --group)
      GROUP="$2"
      shift
      ;;
    --help)
      print_usage
      exit 0
      ;;
    *)
      ;;
  esac
  shift
done

if [ -z "$VERSION" ]; then
  print_usage
  exit
fi
echo VERSION=$VERSION

if [ -z "$GROUP" ]; then
 GROUP="org.apache.gobblin"
fi


./gradlew install -Dorg.gradle.parallel=false -Pversion=$VERSION -Pgroup=$GROUP
