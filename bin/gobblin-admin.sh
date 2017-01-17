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

function print_usage() {
  echo "gobblin-admin.sh [JAVA_OPTION] COMMAND [OPTION]"
  echo "Where JAVA_OPTION can be:"
  echo "  --fwdir <fwd dir>                              Gobblin's dist directory: if not set, taken from \${GOBBLIN_FWDIR}"
  echo "  --logdir <log dir>                             Gobblin's log directory: if not set, taken from \${GOBBLIN_LOG_DIR}"
  echo "  --jars <comma-separated list of job jars>      Job jar(s): if not set, \${GOBBLIN_FWDIR/lib} is examined"
  echo "  --help                                         Display this help and exit"
  echo "COMMAND is one of the following:"
  echo "  jobs|tasks"
  echo "And OPTION are any options associated with the command, as specified by the CLI."
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
    jobs|tasks)
      ACTION="$1"
      break
      ;;
    --fwdir)
      FWDIR="$2"
      shift 2
      ;;
    --logdir)
      LOG_DIR="$2"
      shift 2
      ;;
    --jars)
      JARS="$2"
      shift 2
      ;;
    --help)
      print_usage
      exit 0
      ;;
    *)
      ;;
  esac
done

if [ -z "$ACTION" ]; then
  print_usage
  exit 0
fi

# Source gobblin default vars
[ -f /etc/default/gobblin ] && . /etc/default/gobblin

if [ -z "$JAVA_HOME" ]; then
  die "Environment variable JAVA_HOME not set!"
fi

if [ -n "$FWDIR" ]; then
  export GOBBLIN_FWDIR="$FWDIR"
fi

if [ -z "$GOBBLIN_FWDIR" ]; then
  die "Environment variable FWDIR not set!"
fi

FWDIR_LIB=$GOBBLIN_FWDIR/lib
FWDIR_CONF=$GOBBLIN_FWDIR/conf

# User defined log directory overrides $GOBBLIN_LOG_DIR
if [ -n "$LOG_DIR" ]; then
  export GOBBLIN_LOG_DIR="$LOG_DIR"
fi

if [ -z "$GOBBLIN_LOG_DIR" ]; then
  die "GOBBLIN_LOG_DIR is not set!"
fi

CONFIG_FILE=$FWDIR_CONF/gobblin-standalone.properties

set_user_jars(){
  local separator=''
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
  GOBBLIN_JARS+="$separator$dirname/$jarname"
  separator=':'
}

# Add the absoulte path of the user defined job jars to the GOBBLIN_JARS
set_user_jars "$JARS"

for jar in $(ls -d $FWDIR_LIB/*); do
  if [ "$GOBBLIN_JARS" != "" ]; then
    GOBBLIN_JARS+=":$jar"
  else
    GOBBLIN_JARS=$jar
  fi
done

CLASSPATH=":$GOBBLIN_JARS:$FWDIR_CONF"

COMMAND="$JAVA_HOME/bin/java -Xmx1024m -Xms256m "
COMMAND+="-XX:+UseCompressedOops "
COMMAND+="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$GOBBLIN_LOG_DIR/ "
COMMAND+="-Xloggc:$GOBBLIN_LOG_DIR/gobblin-gc.log "
COMMAND+="-Dgobblin.logs.dir=$GOBBLIN_LOG_DIR "
COMMAND+="-Dlog4j.configuration=file://$FWDIR_CONF/log4j-standalone.xml "
COMMAND+="-cp $CLASSPATH "
COMMAND+="gobblin.cli.Cli $@"
$COMMAND