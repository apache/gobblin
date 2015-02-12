#!/bin/bash

ACTION=$1
shift
PARAMETERS=$@

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

usage() {
  echo "Usage: state-store-utils.sh migrate <fs uri> <src store root dir> <dest store root dir>"
  echo "  OR"
  echo "Usage: state-store-utils.sh check -p <gobblin system configuration file> -n <job name> -i <job ID> [-a] [-kc]"
}

migrate() {
  echo "Running command:"
  echo "java -cp $CLASSPATH gobblin.runtime.util.StateStoreMigrationUtil $PARAMETERS"
  java -cp $CLASSPATH gobblin.runtime.util.StateStoreMigrationUtil $PARAMETERS
}

check() {
  echo "Running command:"
  echo "java -cp $CLASSPATH gobblin.runtime.util.JobStateToJsonConverter $PARAMETERS"
  java -cp $CLASSPATH gobblin.runtime.util.JobStateToJsonConverter $PARAMETERS
}

case $ACTION in
  "help")
    usage
    ;;
  "migrate")
    migrate
    ;;
  "check")
    check
    ;;
  *)
    usage
    exit 1
    ;;
esac
