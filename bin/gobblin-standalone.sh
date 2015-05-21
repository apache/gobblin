#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
FWDIR_LIB=$FWDIR/lib
FWDIR_CONF=$FWDIR/conf

function print_usage(){
  echo "gobblin-standalone.sh <start | status | restart | stop> [OPTION]"
  echo "Where OPTION can be:"
  echo "  --workdir <job work dir>                       Gobblin's base work directory: if not set, taken from \${GOBBLIN_WORK_DIR}"
  echo "  --jars <comma-separated list of job jars>      Job jar(s): if not set, "$FWDIR_LIB" is examined"
  echo "  --conf <directory of job configuration files>  Directory of job configuration files: if not set, taken from ${GOBBLIN_JOB_CONFIG_DIR}"
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
    start|stop|restart|status)
      ACTION="$1"
      ;;
    --workdir)
      WORK_DIR="$2"
      shift
      ;;
    --jars)
      JARS="$2"
      shift
      ;;
    --conf)
      JOB_CONFIG_DIR="$2"
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

if [ -z "$JAVA_HOME" ]; then
  die "Environment variable JAVA_HOME not set!"
fi

check=false
if [ "$ACTION" == "start" ] || [ "$ACTION" == "restart" ]; then
  check=true
fi

# User defined job configuration directory overrides $GOBBLIN_JOB_CONFIG_DIR
if [ -n "$JOB_CONFIG_DIR" ]; then
  export GOBBLIN_JOB_CONFIG_DIR="$JOB_CONFIG_DIR"
fi

if [ -z "$GOBBLIN_JOB_CONFIG_DIR" ] && [ "$check" == true ]; then
  die "Environment variable GOBBLIN_JOB_CONFIG_DIR not set!"
fi

# User defined work directory overrides $GOBBLIN_WORK_DIR
if [ -n "$WORK_DIR" ]; then
  export GOBBLIN_WORK_DIR="$WORK_DIR"
fi

if [ -z "$GOBBLIN_WORK_DIR" ] && [ "$check" == true ]; then
  die "GOBBLIN_WORK_DIR is not set!"
fi

. $FWDIR_CONF/gobblin-env.sh

CONFIG_FILE=$FWDIR_CONF/gobblin-standalone.properties

PID="$GOBBLIN_WORK_DIR/.gobblin-pid"

if [ -f "$PID" ]; then
  PID_VALUE=`cat $PID` > /dev/null 2>&1
else
  PID_VALUE=""
fi

if [ ! -d "$FWDIR/logs" ]; then
  mkdir "$FWDIR/logs"
fi

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

start() {
  for jar in $(ls -d $FWDIR_LIB/*); do
    if [ "$GOBBLIN_JARS" != "" ]; then
      GOBBLIN_JARS+=":$jar"
    else
      GOBBLIN_JARS=$jar
    fi
  done

  CLASSPATH="$GOBBLIN_JARS:$FWDIR_CONF"

  echo "Starting Gobblin standalone daemon"
  COMMAND="$JAVA_HOME/bin/java -Xmx2g -Xms1g "
  COMMAND+="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC "
  COMMAND+="-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution "
  COMMAND+="-XX:+UseCompressedOops "
  COMMAND+="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$FWDIR/logs/ "
  COMMAND+="-Xloggc:$FWDIR/logs/gobblin-gc.log "
  COMMAND+="-Dgobblin.logs.dir=$FWDIR/logs "
  COMMAND+="-Dlog4j.configuration=file://$FWDIR_CONF/log4j-standalone.xml "
  COMMAND+="-cp $CLASSPATH "
  COMMAND+="-Dorg.quartz.properties=$FWDIR_CONF/quartz.properties "
  COMMAND+="gobblin.scheduler.SchedulerDaemon $CONFIG_FILE"
  echo "Running command:"
  echo "$COMMAND"
  nohup $COMMAND & echo $! > $PID
}

stop() {
  if [ -f "$PID" ]; then
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
  if [ -f "$PID" ]; then
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

case "$ACTION" in
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
    print_usage
    exit 1
    ;;
esac
