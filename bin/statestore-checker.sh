#!/bin/bash

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

java -cp $CLASSPATH gobblin.runtime.util.JobStateToJsonConverter $@
