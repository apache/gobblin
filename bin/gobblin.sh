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

calling_dir() {
  echo "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
}
classpath() {
  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  for i in `ls $DIR/../lib`
  do
      if [[ $i != hadoop* ]]
      then
        CLASSPATH=${CLASSPATH:+${CLASSPATH}:}$DIR/../lib/$i
      else
        HADOOP_CLASSPATH=${HADOOP_CLASSPATH:+${HADOOP_CLASSPATH}:}$DIR/../lib/$i
      fi
  done

  if [ ! -z "$HADOOP_HOME" ] && [ -f $HADOOP_HOME/bin/hadoop ]
  then
    HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
  fi

  CLASSPATH=$CLASSPATH:$HADOOP_CLASSPATH

  if [ ! -z "$GOBBLIN_ADDITIONAL_JARS" ]
  then
     CLASSPATH=$GOBBLIN_ADDITIONAL_JARS:$CLASSPATH
  fi

  echo $CLASSPATH
}

for i in "$@"
do
  case "$1" in
    "classpath")
      classpath
      exit
  esac
done

CLASSPATH=$(classpath)
if [ -z "$GOBBLIN_LOG4J_CONFIGURATION" ]
then
  GOBBLIN_LOG4J_CONFIGURATION=$(calling_dir)/../conf/log4j.properties
fi

java -Dlog4j.configuration=file:$GOBBLIN_LOG4J_CONFIGURATION -cp "$CLASSPATH" $GOBBLIN_OPTS org.apache.gobblin.runtime.cli.GobblinCli $@
