#!/bin/bash

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

java -Dlog4j.configuration=file:$GOBBLIN_LOG4J_CONFIGURATION -cp "$CLASSPATH" $GOBBLIN_OPTS gobblin.runtime.cli.GobblinCli $@
