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
############### Run Gobblin Jobs on Hadoop MR ################
##############################################################

# Set during the distribution build
GOBBLIN_VERSION=@project.version@

FWDIR="$(cd `dirname $0`/..; pwd)"
FWDIR_LIB=$FWDIR/lib
FWDIR_CONF=$FWDIR/conf
FWDIR_BIN=$FWDIR/bin

function print_usage(){
  echo "Usage: gobblin-mapreduce.sh [OPTION] --conf <job configuration file>"
  echo "Where OPTION can be:"
  echo "  --jt <job tracker / resource manager URL>      Job submission URL: if not set, taken from \${HADOOP_HOME}/conf"
  echo "  --fs <file system URL>                         Target file system: if not set, taken from \${HADOOP_HOME}/conf"
  echo "  --jars <comma-separated list of job jars>      Job jar(s): if not set, \"$FWDIR_LIB\" is examined"
  echo "  --workdir <job work dir>                       Gobblin's base work directory: if not set, taken from \${GOBBLIN_WORK_DIR}"
  echo "  --projectversion <version>                     Gobblin version to be used. If set, overrides the distribution build version"
  echo "  --logdir <log dir>                             Gobblin's log directory: if not set, taken from \${GOBBLIN_LOG_DIR} if present. Otherwise \"$FWDIR/logs\" is used"
  echo "  --help                                         Display this help and exit"
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
    --jt)
      JOB_TRACKER_URL="$2"
      shift
      ;;
    --fs)
      FS_URL="$2"
      shift
      ;;
    --jars)
      JARS="$2"
      shift
      ;;
    --workdir)
      WORK_DIR="$2"
      shift
      ;;
    --logdir)
      LOG_DIR="$2"
      shift
      ;;
    --conf)
      JOB_CONFIG_FILE="$2"
      shift
      ;;
    --projectversion)
      GOBBLIN_VERSION="$2"
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

if [ -z "$JOB_CONFIG_FILE" ]; then
  die "No job configuration file set!"
fi

# User defined work directory overrides $GOBBLIN_WORK_DIR
if [ -n "$WORK_DIR" ]; then
  export GOBBLIN_WORK_DIR="$WORK_DIR"
fi

if [ -z "$GOBBLIN_WORK_DIR" ]; then
  die "GOBBLIN_WORK_DIR is not set!"
fi

# User defined log directory overrides $GOBBLIN_LOG_DIR
if [ -n "$LOG_DIR" ]; then
  export GOBBLIN_LOG_DIR="$LOG_DIR"
fi

if [ -z "$GOBBLIN_LOG_DIR" ]; then
  GOBBLIN_LOG_DIR="$FWDIR/logs"
fi

. $FWDIR_BIN/gobblin-env.sh

USER_JARS=""
separator=''
set_user_jars(){
  if [ -n "$1" ]; then
    IFS=','
    read -ra userjars <<< "$1"
    for userjar in ${userjars[@]}; do
      add_user_jar "$userjar"
     done
    unset IFS
  fi
}

add_user_jar(){
  local dirname=`dirname "$1"`
  local jarname=`basename "$1"`
  dirname=`cd "$dirname">/dev/null; pwd`
  USER_JARS+="$separator$dirname/$jarname"
  separator=','
}

# Add the absolute path of the user defined job jars to the LIBJARS first
set_user_jars "$JARS"

# Jars Gobblin runtime depends on
# Please note that both versions of the metrics jar are required.
function join { local IFS="$1"; shift; echo "$*"; }
LIBJARS=(
  $USER_JARS
  $FWDIR_LIB/gobblin-api-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-avro-json-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-codecs-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-core-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-core-base-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-crypto-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-crypto-provider-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-data-management-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-metastore-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-metrics-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-metrics-base-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-metadata-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/gobblin-utility-$GOBBLIN_VERSION.jar
  $FWDIR_LIB/avro-1.8.1.jar
  $FWDIR_LIB/avro-mapred-1.8.1.jar
  $FWDIR_LIB/commons-lang3-3.4.jar
  $FWDIR_LIB/config-1.2.1.jar
  $FWDIR_LIB/data-11.0.0.jar
  $FWDIR_LIB/gson-2.6.2.jar
  $FWDIR_LIB/guava-15.0.jar
  $FWDIR_LIB/guava-retrying-2.0.0.jar
  $FWDIR_LIB/joda-time-2.9.3.jar
  $FWDIR_LIB/javassist-3.18.2-GA.jar
  $FWDIR_LIB/kafka_2.11-0.8.2.2.jar
  $FWDIR_LIB/kafka-clients-0.8.2.2.jar
  $FWDIR_LIB/metrics-core-2.2.0.jar
  $FWDIR_LIB/metrics-core-3.1.0.jar
  $FWDIR_LIB/metrics-graphite-3.1.0.jar
  $FWDIR_LIB/scala-library-2.11.8.jar
  $FWDIR_LIB/influxdb-java-2.1.jar
  $FWDIR_LIB/okhttp-2.4.0.jar
  $FWDIR_LIB/okio-1.4.0.jar
  $FWDIR_LIB/retrofit-1.9.0.jar
  $FWDIR_LIB/reflections-0.9.10.jar
)
LIBJARS=$(join , "${LIBJARS[@]}")

# Add libraries to the Hadoop classpath
GOBBLIN_DEP_JARS=`echo "$USER_JARS" | tr ',' ':' `
for jarFile in `ls $FWDIR_LIB/*`
do
  GOBBLIN_DEP_JARS=${GOBBLIN_DEP_JARS}:$jarFile
done

# Honor Gobblin dependencies
export HADOOP_USER_CLASSPATH_FIRST=true
export HADOOP_CLASSPATH=$GOBBLIN_DEP_JARS:$HADOOP_CLASSPATH

GOBBLIN_CONFIG_FILE=$FWDIR_CONF/gobblin-mapreduce.properties

JT_COMMAND=$([ -z $JOB_TRACKER_URL ] && echo "" || echo "-jt $JOB_TRACKER_URL")
FS_COMMAND=$([ -z $FS_URL ] && echo "" || echo "-fs $FS_URL")

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dgobblin.logs.dir=$GOBBLIN_LOG_DIR -Dlog4j.configuration=file:$FWDIR_CONF/log4j-mapreduce.xml"

# Launch the job to run on Hadoop
$HADOOP_BIN_DIR/hadoop jar \
        $FWDIR_LIB/gobblin-runtime-$GOBBLIN_VERSION.jar \
        org.apache.gobblin.runtime.mapreduce.CliMRJobLauncher \
        -D mapreduce.user.classpath.first=true \
        -D mapreduce.job.user.classpath.first=true \
        $JT_COMMAND \
        $FS_COMMAND \
        -libjars $LIBJARS \
        -sysconfig $GOBBLIN_CONFIG_FILE \
        -jobconfig $JOB_CONFIG_FILE
