#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# JAVA_HOME is required.
if [[ -z "$JAVA_HOME" ]]; then
    echo -e "\nError: Environment variable JAVA_HOME not set!\n"
    exit 1
fi

# global vars

GOBBLIN_VERSION=@project.version@
GOBBLIN_HOME="$(cd `dirname $0`/..; pwd)"
GOBBLIN_LIB=${GOBBLIN_HOME}/lib
GOBBLIN_BIN=${GOBBLIN_HOME}/bin
GOBBLIN_LOGS=${GOBBLIN_HOME}/logs
GOBBLIN_CONF=''

#sourcing basic gobblin env vars like GOBBLIN_HOME and GOBBLIN_LIB
. ${GOBBLIN_BIN}/gobblin-env.sh

CLUSTER_NAME="gobblin_cluster"
JVM_OPTS="-Xmx1g -Xms512m"
LOG4J_FILE_PATH=''
LOG4J_OPTS=''
GOBBLIN_MODE=''
ACTION=''
JVM_FLAGS=''
EXTRA_JARS=''
VERBOSE=0
ENABLE_GC_LOGS=0
CMD_PARAMS=''
GOBBLIN_COMMAND_MODE="GOBBLIN_COMMAND"
GOBBLIN_EXE_MODE="GOBBLIN_SERVICE"

# Gobblin Commands, Modes & respective Classes
# Commands
ADMIN_CMD='admin'
CLI_CMD='cli'
STATESTORE_CHECK_CMD='statestore-check'
STATESTORE_CLEAN_CMD='statestore-clean'
HISTORYSTORE_MANAGER_CMD='historystore-manager'
CLASSPATH_CMD='classpath'

# Execution Modes
STANDALONE_MODE='standalone'
CLUSTER_MASTER_MODE='cluster-master'
CLUSTER_WORKER_MODE='cluster-worker'
AWS_MODE='aws'
YARN_MODE='yarn'
MAPREDUCE_MODE='mapreduce'
SERVICE_MODE='service'

# Command class
ADMIN_CLASS="org.apache.gobblin.cli.Cli"
CLI_CLASS='org.apache.gobblin.runtime.cli.GobblinCli'
STATESTORE_CHECK_CLASS='org.apache.gobblin.runtime.util.JobStateToJsonConverter'
STATESTORE_CLEAN_CLASS='org.apache.gobblin.metastore.util.StateStoreCleaner'
HISTORYSTORE_MANAGER_CLASS='org.apache.gobblin.metastore.util.DatabaseJobHistoryStoreSchemaManager'

# Service Class
STANDALONE_CLASS='org.apache.gobblin.scheduler.SchedulerDaemon'
CLUSTER_MASTER_CLASS='org.apache.gobblin.cluster.GobblinClusterManager'
CLUSTER_WORKER_CLASS='org.apache.gobblin.cluster.GobblinTaskRunner'
AWS_CLASS='org.apache.gobblin.aws.GobblinAWSClusterLauncher'
YARN_CLASS='org.apache.gobblin.yarn.GobblinYarnAppLauncher'
MAPREDUCE_CLASS='org.apache.gobblin.runtime.mapreduce.CliMRJobLauncher'
SERVICE_CLASS='org.apache.gobblin.service.modules.core.GobblinServiceManager'


function print_usage() {
    echo "gobblin.sh  <command> <params>"
    echo "gobblin.sh  <execution-mode> <start|stop|status>"
    echo ""
    echo "Argument Options:"
    echo "    <commands>                       $ADMIN_CMD, $CLI_CMD, $STATESTORE_CHECK_CMD, $STATESTORE_CLEAN_CMD, $HISTORYSTORE_MANAGER_CMD, $CLASSPATH_CMD"
    echo "    <execution-mode>                 $STANDALONE_MODE, $CLUSTER_MASTER_MODE, $CLUSTER_WORKER_MODE, $AWS_MODE, $YARN_MODE, $MAPREDUCE_MODE, $SERVICE_MODE."
    echo "    --cluster-name                   cluster name, also used by helix & other services. ( default: $CLUSTER_NAME)."
    echo "    --conf-dir <path-of-conf-dir>    default is '\$GOBBLIN_HOME/conf/<exe-mode-name>'."
    echo "    --log4j-conf <path-of-conf-file> default is '\$GOBBLIN_HOME/conf/<exe-mode-name>/log4j.properties'."
    echo "    --jt <resource manager URL>      Only for mapreduce mode: Job submission URL, if not set, taken from \${HADOOP_HOME}/conf."
    echo "    --fs <file system URL>           Only for mapreduce mode: Target file system, if not set, taken from \${HADOOP_HOME}/conf."
    echo "    --jvmopts <jvm or gc options>    String containing JVM flags to include, in addition to \"$JVM_OPTS\"."
    echo "    --jars <csv list of extra jars>  Column-separated list of extra jars to put on the CLASSPATH."
    echo "    --enable-gc-logs                 enables gc logs & dumps."
    echo "    --help                           Display this help."
    echo "    --verbose                        Display full command used to start the process."
    echo "                                     Gobblin Version: $GOBBLIN_VERSION"
}

# TODO: use getopts
shopt -s nocasematch
for i in "$@"
do
    case "$1" in
        "$CLASSPATH_CMD" | "$ADMIN_CMD" | "$CLI_CMD" | "$STATESTORE_CHECK_CMD" | "$STATESTORE_CLEAN_CMD" | "$HISTORYSTORE_MANAGER_CLASS" )
            GOBBLIN_MODE_TYPE=$GOBBLIN_COMMAND_MODE
            GOBBLIN_MODE="$1"
        ;;
        "$STANDALONE_MODE" | "$CLUSTER_MASTER_MODE" | "$CLUSTER_WORKER_MODE" | "$AWS_MODE" | "$YARN_MODE" | "$MAPREDUCE_MODE")
            GOBBLIN_MODE_TYPE=$GOBBLIN_EXE_MODE
            GOBBLIN_MODE="$1"
        ;;
        start | stop | status)
            ACTION="$1"
        ;;
        --jvmflags)
            JVM_FLAGS="$2"
            shift
        ;;
        --conf-dir)
            USER_CONF_DIR="$2"
            shift
        ;;
        --log4j-conf)
            USER_LOG4J_FILE="$2"
            shift
        ;;
        --jars)
            EXTRA_JARS="$2"
            shift
        ;;
        --enable-gc-logs)
            ENABLE_GC_LOGS=1
        ;;
        --cluster-name)
            CLUSTER_NAME="$2"
            shift
        ;;
        --help)
            print_usage
            exit 0
        ;;
        --verbose)
            VERBOSE=1
        ;;
        --jt)
            JOB_TRACKER_URL="$2"
            shift
        ;;
        --fs)
            FS_URL="$2"
            shift
        ;;
        *)
            CMD_PARAMS="$CMD_PARAMS $1"
        ;;
    esac
    shift
done

# derived based on input from user, $GOBBLIN_MODE
PID_FILE_NAME=".gobblin-$GOBBLIN_MODE.pid"
PID_FILE="$GOBBLIN_HOME/$PID_FILE_NAME"


# for gobblin commands, the action is always 'start'
if [[ "$GOBBLIN_MODE_TYPE" == "$GOBBLIN_COMMAND_MODE" ]]; then
    ACTION='start'
fi

# JVM Flags
if [[ -n "$JVM_FLAGS" ]]; then
    JVM_OPTS="$JVM_OPTS $JVM_FLAGS"
fi

# gobblin config
if [[ -n "$USER_CONF_DIR" ]]; then
    GOBBLIN_CONF=$USER_CONF_DIR
else
    GOBBLIN_CONF=${GOBBLIN_HOME}/conf/${GOBBLIN_MODE}
fi

#log4j config file


if [[ -n "$USER_LOG4J_FILE" ]]; then
    LOG4J_FILE_PATH=file://${USER_LOG4J_FILE}
    LOG4J_OPTS="-Dlog4j.configuration=$LOG4J_FILE_PATH"
elif [[ -f ${GOBBLIN_CONF}/log4j.properties ]]; then
    LOG4J_FILE_PATH=file://${GOBBLIN_CONF}/log4j.properties
    LOG4J_OPTS="-Dlog4j.configuration=$LOG4J_FILE_PATH"
fi


GC_OPTS=''
if [[ ${ENABLE_GC_LOGS} -eq 1 ]]; then
    GC_OPTS+="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseCompressedOops "
    GC_OPTS+="-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution "
    GC_OPTS+="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$GOBBLIN_LOGS/ "
    GC_OPTS+="-Xloggc:$GOBBLIN_LOGS/gobblin-$GOBBLIN_MODE-gc.log "
fi

export HADOOP_USER_CLASSPATH_FIRST=true

function build_classpath(){
    GOBBLIN_CLASSPATH=''
    # Build classpth
    GOBBLIN_JARS=''
    GOBBLIN_HADOOP_JARS=''
    GOBBLIN_CLASSPATH=''

    for jarFile in `ls ${GOBBLIN_LIB}/*`
    do
        if [[ $jarFile == hadoop* ]]; then
            GOBBLIN_HADOOP_JARS=${GOBBLIN_HADOOP_JARS}:${jarFile}
        else
            GOBBLIN_JARS=${GOBBLIN_JARS}:${jarFile}
        fi
    done

    # just removing first colon if present
    GOBBLIN_HADOOP_JARS=${GOBBLIN_HADOOP_JARS#:}
    GOBBLIN_JARS=${GOBBLIN_JARS#:}

    if [[ -n "$HADOOP_HOME" ]]; then
        HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
        GOBBLIN_CLASSPATH=${GOBBLIN_JARS}:${HADOOP_CLASSPATH}
    else
        echo "WARN: HADOOP_HOME is not defined. Gobblin Hadoop libs will be used in classpath."
        HADOOP_CLASSPATH=${GOBBLIN_HADOOP_JARS}
    fi

    GOBBLIN_CLASSPATH=${GOBBLIN_JARS}:${HADOOP_CLASSPATH}

    if [[ -n "$EXTRA_JARS" ]]; then
        GOBBLIN_CLASSPATH=${GOBBLIN_CLASSPATH}:"$EXTRA_JARS"
    fi

    GOBBLIN_CLASSPATH=${GOBBLIN_CONF}:${GOBBLIN_CLASSPATH}

}

function start() {
    # build and set the classpath in variable: GOBBLIN_CLASSPATH
    build_classpath

    LOG_OUT_FILE="${GOBBLIN_LOGS}/${GOBBLIN_MODE}.out"
    LOG_ERR_FILE="${GOBBLIN_LOGS}/${GOBBLIN_MODE}.err"

    # for all gobblin commands
    if [[ "$GOBBLIN_MODE_TYPE" == "$GOBBLIN_COMMAND_MODE" ]]; then
        if [[ "$GOBBLIN_MODE" = "$CLASSPATH_CMD" ]]; then
            # not adding anything in echo here so that it can be used for getting classpath from any other command
            echo "$GOBBLIN_CLASSPATH"
        else
            if [[ "$GOBBLIN_MODE" = "$ADMIN_CMD" ]]; then
                CLASS_N_ARGS="$ADMIN_CLASS"
            elif [[ "$GOBBLIN_MODE" = "$CLI_CMD" ]]; then
                CLASS_N_ARGS="$CLI_CLASS"
            elif [[ "$GOBBLIN_MODE" = "$STATESTORE_CHECK_CMD" ]]; then
                CLASS_N_ARGS="$STATESTORE_CHECK_CLASS"
            elif [[ "$GOBBLIN_MODE" = "$STATESTORE_CLEAN_CMD" ]]; then
                CLASS_N_ARGS="$STATESTORE_CLEAN_CLASS"
            elif [[ "$GOBBLIN_MODE" = "$HISTORYSTORE_MANAGER_CMD" ]]; then
                CLASS_N_ARGS="$HISTORYSTORE_MANAGER_CLASS"
            fi

            if [[ $VERBOSE -eq 1 ]]; then
                echo "Running command: $JAVA_HOME/bin/java $GC_OPTS $JVM_OPTS -cp $GOBBLIN_CLASSPATH  $CLASS_N_ARGS $CMD_PARAMS";
            fi

            # execute the command
            if [[ $VERBOSE -eq 1 ]]; then
                echo "Running command: $JAVA_HOME/bin/java $GC_OPTS $JVM_OPTS -cp $GOBBLIN_CLASSPATH  $CLASS_N_ARGS $CMD_PARAMS";
            fi

            $JAVA_HOME/bin/java $GC_OPTS $JVM_OPTS -cp $GOBBLIN_CLASSPATH  $CLASS_N_ARGS $CMD_PARAMS
        fi
    # for all gobblin execution modes
    else
        if [[ "$GOBBLIN_MODE" = "$MAPREDUCE_MODE" ]]; then
            MR_MODE_LIB_JARS="gobblin-api-$GOBBLIN_VERSION.jar,gobblin-avro-json-$GOBBLIN_VERSION.jar,
                    gobblin-codecs-$GOBBLIN_VERSION.jar,gobblin-core-$GOBBLIN_VERSION.jar,gobblin-core-base-$GOBBLIN_VERSION.jar,
                    gobblin-crypto-$GOBBLIN_VERSION.jar,gobblin-crypto-provider-$GOBBLIN_VERSION.jar,gobblin-data-management-$GOBBLIN_VERSION.jar,
                    gobblin-metastore-$GOBBLIN_VERSION.jar,gobblin-metrics-$GOBBLIN_VERSION.jar,gobblin-metrics-base-$GOBBLIN_VERSION.jar,
                    gobblin-metadata-$GOBBLIN_VERSION.jar,gobblin-utility-$GOBBLIN_VERSION.jar,avro-1.8.1.jar,avro-mapred-1.8.1.jar,
                    commons-lang3-3.4.jar,config-1.2.1.jar,data-11.0.0.jar,gson-2.6.2.jar,guava-15.0.jar,guava-retrying-2.0.0.jar,
                    joda-time-2.9.3.jar,javassist-3.18.2-GA.jar,kafka_2.11-0.8.2.2.jar,kafka-clients-0.8.2.2.jar,metrics-core-2.2.0.jar,
                    metrics-core-3.2.3.jar,metrics-graphite-3.2.3.jar,scala-library-2.11.8.jar,influxdb-java-2.1.jar,okhttp-2.4.0.jar,
                    okio-1.4.0.jar,reactive-streams-1.0.0.jar,retrofit-1.9.0.jar,reflections-0.9.10.jar"

            MR_MODE_LIB_JARS="${MR_MODE_LIB_JARS},$EXTRA_JARS"
            JT_COMMAND=$([ -z $JOB_TRACKER_URL ] && echo "" || echo "-jt $JOB_TRACKER_URL")
            FS_COMMAND=$([ -z $FS_URL ] && echo "" || echo "-fs $FS_URL")
            GOBBLIN_COMMAND="hadoop jar $GOBBLIN_LIB/gobblin-runtime-$GOBBLIN_VERSION.jar $MAPREDUCE_CLASS \
                                        -D mapreduce.user.classpath.first=true -D mapreduce.job.user.classpath.first=true \
                                        $JT_COMMAND $FS_COMMAND \
                                        -libjars $MR_MODE_LIB_JARS \
                                        -sysconfig $GOBBLIN_CONF/application.properties \
                                        -jobconfig $GOBBLIN_CONF/application.properties"
        else
            CLASS_N_ARGS=''
            if [[ "$GOBBLIN_MODE" = "$STANDALONE_MODE" ]]; then
                CLASS_N_ARGS="$STANDALONE_CLASS $GOBBLIN_CONF/application.conf"

            elif [[ "$GOBBLIN_MODE" = "$AWS_MODE" ]]; then
                CLASS_N_ARGS="$AWS_CLASS"

            elif [[ "$GOBBLIN_MODE" = "$YARN_MODE" ]]; then
                GOBBLIN_CLASSPATH="${GOBBLIN_CLASSPATH}:${HADOOP_YARN_HOME}/lib"
                CLASS_N_ARGS="$YARN_CLASS"

            elif [[ "$GOBBLIN_MODE" = "$CLUSTER_MASTER_MODE" ]]; then
                CLASS_N_ARGS="$CLUSTER_MASTER_CLASS --standalone_cluster true --app_name $CLUSTER_NAME"

            elif [[ "$GOBBLIN_MODE" = "$SERVICE_MODE" ]]; then
                CLASS_N_ARGS="$SERVICE_CLASS --service_name Gobblin-$SERVICE_MODE"

            elif [[ "$GOBBLIN_MODE" = "$CLUSTER_WORKER_MODE" ]]; then
                #Find largest worker id and use next one to start worker in incremental order
                LAST_WORKER_ID=$(ps aux | grep -v grep | grep -Po "($CLUSTER_WORKER_CLASS)(.*)(cluster-worker.\K([0-9]+))" | sort --version-sort | tail -1)
                WORKER_ID=$((LAST_WORKER_ID+1))
                LOG_OUT_FILE="${GOBBLIN_LOGS}/${GOBBLIN_MODE}.$WORKER_ID.out"
                LOG_ERR_FILE="${GOBBLIN_LOGS}/${GOBBLIN_MODE}.$WORKER_ID.err"
                CLASS_N_ARGS="$CLUSTER_WORKER_CLASS --app_name $CLUSTER_NAME --helix_instance_name ${GOBBLIN_MODE}.$WORKER_ID"
            else
                echo "Invalid gobblin command or execution mode... [EXITING]"
                exit 1
            fi
            GOBBLIN_COMMAND="$JAVA_HOME/bin/java -cp $GOBBLIN_CLASSPATH $GC_OPTS $JVM_OPTS $LOG4J_OPTS $CLASS_N_ARGS"
        fi

        # execute the command
        if [[ $VERBOSE -eq 1 ]]; then
            echo "Running command: $GOBBLIN_COMMAND";
        fi

        nohup $GOBBLIN_COMMAND 1>> ${LOG_OUT_FILE} 2>> ${LOG_ERR_FILE} &
        PID=$!
        echo $PID >> $PID_FILE
        if [[ $? != 0 ]]; then
            echo "Starting the Gobblin $GOBBLIN_MODE process... [FAILED]"
        else
            echo "Started the Gobblin $GOBBLIN_MODE process [pid: $PID] ... [DONE]"
        fi

    fi
}

function stop() {
    #    echo "Stopping the Gobblin $MODE_TYPE process..."
    PID=''
    if [[ ! -f $PID_FILE ]]; then
        echo "Gobblin process id file not found at $PID_FILE"
    else
        PID=`tail -1 $PID_FILE`
    fi

    if [[ -z "$PID" ]]; then
        echo "Can not find any running Gobblin $GOBBLIN_MODE process..."
        while true; do
            read -p "Do you want to search gobblin $GOBBLIN_MODE process and stop it? (y/n): " search_and_kill
            case ${search_and_kill} in
                [Yy]*)
                    class_to_search=''
                    if [[ "$GOBBLIN_MODE" = "$MAPREDUCE_MODE" ]]; then
                        class_to_search="$MAPREDUCE_CLASS"
                    elif [[ "$GOBBLIN_MODE" = "$CLI_CMD" ]]; then
                        class_to_search="$CLI_CLASS"
                    elif [[ "$GOBBLIN_MODE" = "$STANDALONE_MODE" ]]; then
                        class_to_search="$STANDALONE_CLASS"
                    elif [[ "$GOBBLIN_MODE" = "$AWS_MODE" ]]; then
                        class_to_search="$AWS_CLASS"
                    elif [[ "$GOBBLIN_MODE" = "$YARN_MODE" ]]; then
                        class_to_search="$YARN_CLASS"
                    elif [[ "$GOBBLIN_MODE" = "$CLUSTER_MASTER_MODE" ]]; then
                        class_to_search="$CLUSTER_MASTER_CLASS"
                    elif [[ "$GOBBLIN_MODE" = "$CLUSTER_WORKER_MODE" ]]; then
                        class_to_search=$CLUSTER_WORKER_CLASS
                    fi

                    if [[ -z "$class_to_search" ]]; then
                        echo "Could not figure out process to search for MODE: $GOBBLIN_MODE...[ABORTED]"
                        exit
                    fi

                    PID=$(ps aux | grep "$class_to_search" | grep -v grep | awk '{print $2}')
                    break
                ;;
                [Nn]*)
                    echo "Stopping the Gobblin $GOBBLIN_MODE process... [ABORTED]"
                    exit
                ;;
                *)
                    echo "Please answer yes or no."
                ;;
            esac
        done
    fi

    if [[ -n "$PID" ]]; then
        if kill -0 $PID > /dev/null 2>&1; then
            kill $PID
            printf "Stopping the Gobblin $GOBBLIN_MODE process (pid: $PID)... "; sleep 1; printf "[DONE]\n"
        else
            echo "Gobblin $GOBBLIN_MODE process (pid: $PID) is not running."
        fi
        # remove the pid from pid_file, and remove the file if no more pid's left.
        sed -i '' '$ d' $PID_FILE
        if [[ -s $PID_FILE ]]; then
            rm $PID_FILE;
        fi
    else
        echo "Can not find any running Gobblin $GOBBLIN_MODE process...[FAILED]"
    fi
}

function status() {
    #    echo "Checking for Gobblin $MODE_TYPE status ..."
    if [[ -e ${PID_FILE} ]]; then
        PID=`cat $PID_FILE`
    fi
    if [[ -z ${PID} ]]; then
        echo "Gobblin $GOBBLIN_MODE process id not found, probably it is not running."
    elif ps -p ${PID} > /dev/null; then
        echo "Gobblin $GOBBLIN_MODE process is running... [ pid: $PID ]."
    else
        echo "Gobblin $GOBBLIN_MODE process is not running. [ last known pid: ${PID}]."
    fi
}


case "$ACTION" in
    "start")
        start
    ;;
    "stop")
        stop
    ;;
    "status")
        status
    ;;
    "restart")
        stop
        sleep 2
        start
    ;;
    *)
        print_usage
    ;;
esac

