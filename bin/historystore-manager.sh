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


FWDIR="$(cd `dirname $0`/..; pwd)"

GOBBLIN_JARS=""
for jar in $(ls -d $FWDIR/lib/*); do
  if [ "$GOBBLIN_JARS" != "" ]; then
    GOBBLIN_JARS+=":$jar"
  else
    GOBBLIN_JARS=$jar
  fi
done

CLASSPATH=$GOBBLIN_JARS
CLASSPATH+=":$FWDIR/conf"

if [[ $# -gt 0 ]]; then
  action=$1
  options=( "$@" );
  args=( "$@" );
  unset options[$1];
  unset args[$1];
fi

index=1
for var in "${options[@]}"
do
  if [[ $var == -D=* ]]; then
    unset options[$index];
  fi
  index=$((index+1))
done
index=1
for var in "${args[@]}"
do
  if [[ $var != -D=* ]]; then
    unset args[$index];
  fi
  index=$((index+1))
done

java "${options[@]}" -cp $CLASSPATH gobblin.metastore.util.DatabaseJobHistoryStoreSchemaManager $action "${args[@]}"
