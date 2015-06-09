#!/bin/bash

function print_usage() {
  echo "Usage: statestore-utils.sh <check | cleanup> <configuration properties file>"
}

function die() {
  print_usage
  exit 1
}

if [ "$#" -ne 2 ]; then
  die
fi

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

function check() {
  java -cp $CLASSPATH gobblin.runtime.util.JobStateToJsonConverter $2
}

function cleanup() {
  java -cp $CLASSPATH gobblin.metastore.util.StateStoreCleaner $2
}

ACTION=$1

case "$ACTION" in
  check)
  check
  ;;
  cleanup)
  cleanup
  ;;
esac
