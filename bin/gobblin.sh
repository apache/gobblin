#!/bin/bash

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

java -cp "$CLASSPATH" $GOBBLIN_OPTS gobblin.runtime.cli.GobblinCli $@