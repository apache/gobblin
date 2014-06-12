#!/bin/sh

##############################################################
############### Run Gobblin Jobs on Hadoop MR ################
##############################################################

USAGE="launch-mr-job.sh <job tracker URL> <file system URL> <job configuration file>"

if [ "$#" -ne 3 ]; then
    echo "Usage:"
    echo $USAGE
fi

JOB_TRACKER_URL=$1
FS_URL=$2
JOB_CONFIG_FILE=$3

FWDIR="$(cd `dirname $0`/..; pwd)"

# Jars Gobblin runtime depends on
LIBJARS=$FWDIR/lib/common-api.jar,$FWDIR/lib/metastore.jar,$FWDIR/lib/qualitychecker.jar,\
$FWDIR/lib/source.jar,$FWDIR/lib/converter.jar,$FWDIR/lib/writer.jar,$FWDIR/lib/utility.jar,\
$FWDIR/lib/guava-15.0.jar,$FWDIR/lib/avro-1.7.1.jar,$FWDIR/lib/avro-mapred-1.7.6.jar,\
$FWDIR/lib/metrics-core-3.0.2.jar,$FWDIR/lib/gson-2.2.4.jar,$FWDIR/lib/lumos-common-2.0.0.jar

export HADOOP_CLASSPATH=$FWDIR/lib/*

GOBBLIN_CONFIG_FILE=$FWDIR/conf/gobblin-mr-test.properties

# Launch the job to run on Hadoop
$HADOOP_BIN_DIR/hadoop jar \
        $FWDIR/lib/runtime.jar \
        com.linkedin.uif.runtime.mapreduce.CliMRJobLauncher \
        -jt $JOB_TRACKER_URL \
        -fs $FS_URL \
        -libjars $LIBJARS \
        $GOBBLIN_CONFIG_FILE \
        $JOB_CONFIG_FILE
