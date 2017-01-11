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

mkdir -p mock-couchbase
if [ -d mock-couchbase/.git ];
  then
    git -C mock-couchbase pull
    git -C mock-couchbase checkout b929a9f99a31a233c43bd94b6a696ad966206e02
  else
    git clone https://github.com/couchbase/CouchbaseMock.git mock-couchbase
    git -C mock-couchbase checkout b929a9f99a31a233c43bd94b6a696ad966206e02
  fi
pushd mock-couchbase
if ! mvn package -Dmaven.test.skip=true; then
  echo "Error building mock-couchbase"
  exit 1
fi
popd
