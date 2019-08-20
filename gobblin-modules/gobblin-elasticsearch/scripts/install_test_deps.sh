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

TARGET_DIR="test-elasticsearch"
ES_VERSION=5.6.8
ES_DIR=${TARGET_DIR}/elasticsearch-${ES_VERSION}
echo ${TARGET_DIR}
mkdir -p ${TARGET_DIR}


ES_TAR=${TARGET_DIR}/elasticsearch-${ES_VERSION}.tar.gz
ES_URL=https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${ES_VERSION}.tar.gz
echo ${ES_URL}
echo ${ES_TAR}
if [ -d $ES_DIR ];
then
  echo "Skipping download since version already found at ${ES_DIR}"
  echo "Cleaning up directory"
  rm -rf ${TARGET_DIR}/elasticsearch-${ES_VERSION}
else
  echo "$ES_DIR does not exist, downloading"
  curl -o ${ES_TAR} ${ES_URL}
fi
tar -xzf ${ES_TAR} -C ${TARGET_DIR}
