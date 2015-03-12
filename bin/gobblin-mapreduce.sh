#!/bin/sh

##############################################################
############### Run Gobblin Jobs on Hadoop MR ################
##############################################################

FWDIR="$(cd `dirname $0`/..; pwd)"
FWDIR_LIB=$FWDIR/lib
FWDIR_CONF=$FWDIR/conf

function print_usage(){
  echo "Usage: gobblin-mapreduce.sh [OPTION] --conf <job configuration file>"
  echo "Where OPTION can be:"
  echo "  --jt <job tracker / resource manager URL>      Job submission URL: if not set, taken from \${HADOOP_HOME}/conf"
  echo "  --fs <file system URL>                         Target file system: if not set, taken from \${HADOOP_HOME}/conf"
  echo "  --jars <comma-separated list of job jars>      Job jar(s): if not set, "$FWDIR_LIB" is examined"
  echo "  --workdir <job work dir>                       Gobblin's base work directory: if not set, taken from \${GOBBLIN_WORK_DIR}"
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
    --workdir)
      WORK_DIR="$2"
      shift
      ;;
    --jars)
      JARS="$2"
      shift
      ;;
    --conf)
      JOB_CONFIG_FILE="$2"
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

. $FWDIR_CONF/gobblin-env.sh

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

# Add the absoulte path of the user defined job jars to the LIBJARS first
set_user_jars "$JARS"

# Jars Gobblin runtime depends on
LIBJARS=$USER_JARS$separator$FWDIR_LIB/gobblin-metastore.jar,$FWDIR_LIB/gobblin-metrics.jar,\
$FWDIR_LIB/gobblin-core.jar,$FWDIR_LIB/gobblin-api.jar,$FWDIR_LIB/gobblin-utility.jar,\
$FWDIR_LIB/guava-15.0.jar,$FWDIR_LIB/avro-1.7.6.jar,$FWDIR_LIB/avro-mapred-1.7.6.jar,\
$FWDIR_LIB/metrics-core-3.0.2.jar,$FWDIR_LIB/gson-2.2.4.jar,$FWDIR_LIB/joda-time-1.6.jar,$FWDIR_LIB/data-1.15.9.jar

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

# Launch the job to run on Hadoop
$HADOOP_BIN_DIR/hadoop jar \
        $FWDIR_LIB/gobblin-runtime.jar \
        gobblin.runtime.mapreduce.CliMRJobLauncher \
        -D mapreduce.user.classpath.first=true \
        -D mapreduce.job.user.classpath.first=true \
        $JT_COMMAND \
        $FS_COMMAND \
        -libjars $LIBJARS \
        -sysconfig $GOBBLIN_CONFIG_FILE \
        -jobconfig $JOB_CONFIG_FILE