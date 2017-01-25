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

##############################################################
############ Run Gobblin Compaction on Hadoop MR #############
##############################################################

# Set during the distribution build
GOBBLIN_VERSION=@project.version@

FWDIR="$(cd `dirname $0`/..; pwd)"
FWDIR_LIB=$FWDIR/lib
FWDIR_CONF=$FWDIR/conf
FWDIR_BIN=$FWDIR/bin

function print_usage(){
  echo "Usage: gobblin-compaction.sh [OPTION] --type <compaction type: hive or mr> --conf <compaction configuration file>"
  echo "Where OPTION can be:"
  echo "  --projectversion <version>    Gobblin version to be used. If set, overrides the distribution build version"
  echo "  --logdir <log dir>            Gobblin's log directory: if not set, taken from \${GOBBLIN_LOG_DIR} if present. Otherwise \"$FWDIR/logs\" is used"
  echo "  --help                        Display this help and exit"
}

# Print an error message and exit
function die() {
  echo -e "\nError: $@\n" 1>&2
  print_usage
  exit 1
}

for i in "$@"
do
  case "$1" in
    --type)
      TYPE="$2"
      shift
      ;;
    --conf)
      COMP_CONFIG_FILE="$2"
      shift
      ;;
    --jars)
      JARS="$2"
      shift
      ;;
    --projectversion)
      GOBBLIN_VERSION="$2"
      shift
      ;;
    --logdir)
      LOG_DIR="$2"
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

if ( [ -z "$GOBBLIN_VERSION" ] || [ "$GOBBLIN_VERSION" == "@project.version@" ] ); then
  die "Gobblin project version is not set!"
fi

if [ -z "$COMP_CONFIG_FILE" ]; then
  die "No compaction configuration file set!"
fi

if ( [ -z "$TYPE" ] || ( [ "$TYPE" != "hive" ] && [ "$TYPE" != "mr" ] ) ); then
  die "Invalid compaction type set!"
fi

# User defined log directory overrides $GOBBLIN_LOG_DIR
if [ -n "$LOG_DIR" ]; then
  export GOBBLIN_LOG_DIR="$LOG_DIR"
fi

if [ -z "$GOBBLIN_LOG_DIR" ]; then
  GOBBLIN_LOG_DIR="$FWDIR/logs"
fi

. $FWDIR_BIN/gobblin-env.sh

# Jars Gobblin runtime depends on
function join { local IFS="$1"; shift; echo "$*"; }

# Add libraries to the Hadoop classpath
for jarFile in `ls $FWDIR_LIB/*`
do
  GOBBLIN_DEP_JARS=${GOBBLIN_DEP_JARS}:$jarFile
done

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dgobblin.logs.dir=$GOBBLIN_LOG_DIR -Dlog4j.configuration=file:$FWDIR_CONF/log4j-compaction.xml"
# Honor Gobblin dependencies
export HADOOP_USER_CLASSPATH_FIRST=true
export HADOOP_CLASSPATH=$GOBBLIN_DEP_JARS:$HADOOP_CLASSPATH

if [ "$TYPE" == "hive" ]; then

  $HADOOP_BIN_DIR/hadoop jar \
        $FWDIR_LIB/gobblin-compaction-$GOBBLIN_VERSION.jar \
        "gobblin.compaction.hive.CompactionRunner" \
        --jobconfig $COMP_CONFIG_FILE

else

  LIBJARS=(
    $FWDIR_LIB/avro-1.8.1.jar
    $FWDIR_LIB/avro-mapred-1.8.1.jar
    $FWDIR_LIB/commons-cli-1.3.1.jar
    $FWDIR_LIB/commons-lang3-3.4.jar
    $FWDIR_LIB/gobblin-api-$GOBBLIN_VERSION.jar
    $FWDIR_LIB/gobblin-compaction-$GOBBLIN_VERSION.jar
    $FWDIR_LIB/gobblin-utility-$GOBBLIN_VERSION.jar
    $FWDIR_LIB/guava-15.0.jar
  )
  LIBJARS=$(join , "${LIBJARS[@]}")

  $HADOOP_BIN_DIR/hadoop jar \
        $FWDIR_LIB/gobblin-compaction-$GOBBLIN_VERSION.jar \
        "gobblin.compaction.mapreduce.MRCompactionRunner" \
        -D mapreduce.user.classpath.first=true \
        -D mapreduce.job.user.classpath.first=true \
        -libjars $LIBJARS \
        --jobconfig $COMP_CONFIG_FILE

fi
