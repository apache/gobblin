#!/bin/bash

USAGE="gobblin-standalone.sh <start | status | restart | stop>"

if [ "$#" -ne 1 ]; then
  echo "Usage:"
  echo $USAGE
  exit 1
fi

if [ -z $GOBBLIN_WORK_DIR ]; then
  echo "Environment variable GOBBLIN_WORK_DIR not set"
fi

if [ -z $JAVA_HOME ]; then
  echo "Environment variable JAVA_HOME not set"
fi

ACTION=$1

FWDIR="$(cd `dirname $0`/..; pwd)"

CONFIG_FILE=$FWDIR/conf/gobblin-standalone.properties

PID="$GOBBLIN_WORK_DIR/.gobblin-pid"

if [ -f $PID ]; then
  PID_VALUE=`cat $PID` > /dev/null 2>&1
else
  PID_VALUE=""
fi

if [ ! -d "$FWDIR/logs" ]; then
  mkdir "$FWDIR/logs"
fi

start() {
  GOBBLIN_JARS=""
  for jar in $(ls -d $FWDIR/lib/*); do
    if [ "$GOBBLIN_JARS" != "" ]; then
      GOBBLIN_JARS+=":$jar"
    else
      GOBBLIN_JARS=$jar
    fi
  done

  if [ -z $GOBBLIN_JOB_JARS ]; then
    CLASSPATH=$GOBBLIN_JARS
  else
    CLASSPATH=$GOBBLIN_JARS:$GOBBLIN_JOB_JARS
  fi
  CLASSPATH+=":$FWDIR/conf"

  echo "Starting Gobblin standalone daemon"
  COMMAND="$JAVA_HOME/bin/java -Xmx2g -Xms1g "
  COMMAND+="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC "
  COMMAND+="-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution "
  COMMAND+="-XX:+UseCompressedOops "
  COMMAND+="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$FWDIR/logs/ "
  COMMAND+="-Xloggc:$FWDIR/logs/gobblin-gc.log "
  COMMAND+="-Dgobblin.logs.dir=$FWDIR/logs "
  COMMAND+="-Dlog4j.configuration=file://$FWDIR/conf/log4j-standalone.xml "
  COMMAND+="-cp $CLASSPATH "
  COMMAND+="-Dorg.quartz.properties=$FWDIR/conf/quartz.properties "
  COMMAND+="gobblin.scheduler.SchedulerDaemon $CONFIG_FILE"
  echo "Running command:"
  echo "$COMMAND"
  nohup $COMMAND & echo $! > $PID
}

stop() {
  if [ -f $PID ]; then
    if kill -0 $PID_VALUE > /dev/null 2>&1; then
      echo 'Stopping Gobblin standalone daemon'
      kill $PID_VALUE
      sleep 1
      if kill -0 $PID_VALUE > /dev/null 2>&1; then
        echo "Gobblin standalone daemon did not stop gracefully, killing with kill -9"
        kill -9 $PID_VALUE
      fi
    else
      echo "Process $PID_VALUE is not running"
    fi
  else
    echo "No pid file found"
  fi
}

# Check the status of the process
status() {
  if [ -f $PID ]; then
    echo "Looking into file: $PID"
    if kill -0 $PID_VALUE > /dev/null 2>&1; then
      echo "Gobblin standalone daemon is running with status: "
      ps -ef | grep -v grep | grep $PID_VALUE
    else
      echo "Gobblin standalone daemon is not running"
    fi
  else
    echo "No pid file found"
  fi
}

case $ACTION in
  "start")
    start
    ;;
  "status")
    status
    ;;
  "restart")
    stop
    echo "Sleeping..."
    sleep 1
    start
    ;;
  "stop")
    stop
    ;;
  *)
    echo $usage
    exit 1
    ;;
esac
