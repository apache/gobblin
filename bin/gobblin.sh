#!/bin/bash

calling_dir() {
  echo "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
}
classpath() {
  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  echo "$DIR/../lib/*";
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

java -Dlog4j.configuration=file:$GOBBLIN_LOG4J_CONFIGURATION -cp "$CLASSPATH" $GOBBLIN_OPTS gobblin.runtime.cli.GobblinCli $@
