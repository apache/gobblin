#!/bin/bash

if [ -z $GOBBLIN_WORK_DIR ]; then
	echo "Environment variable GOBBLIN_WORK_DIR not set"
fi

if [ -z $JAVA_HOME ]; then
	echo "Environment variable JAVA_HOME not set"
fi

ACTION=$1

FWDIR="$(cd `dirname $0`/..; pwd)"

CONFIG_FILE=$FWDIR/conf/gobblin-test.properties

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
        gobblin_jars=""
	for jar in $(ls -d $FWDIR/lib/*); do
		if [ "$gobblin_jars" != "" ]; then
			gobblin_jars+=":$jar"
		else
			gobblin_jars=$jar
		fi
	done

	if [ -z $GOBBLIN_JOB_JARS ]; then
		classpath=$gobblin_jars
	else
		classpath=$gobblin_jars:$GOBBLIN_JOB_JARS
	fi
	classpath+=":$FWDIR/conf"

	echo "Starting Gobblin"
	cmd="$JAVA_HOME/bin/java -Xmx512M -Xms256M -Xloggc:$FWDIR/logs/gobblin-gc.log -Dgobblin.logs.dir=$FWDIR/logs -Dlog4j.configuration=file://$FWDIR/conf/log4j-test.xml -cp $classpath com.linkedin.uif.scheduler.SchedulerDaemon $CONFIG_FILE"
	echo "Running command:"
	echo "$cmd"
	nohup $cmd & echo $! > $PID
}

stop() {
	if [ -f $PID ]; then
		if kill -0 $PID_VALUE > /dev/null 2>&1; then
			echo 'Stopping Gobblin'
			kill $PID_VALUE
			sleep 1
			if kill -0 $PID_VALUE > /dev/null 2>&1; then
				echo "Gobblin did not stop gracefully, killing with kill -9"
				kill -9 $PID_VALUE
			fi
		else
			echo "Process $PID_VALUE is not running"
		fi
	else
		echo "No pid file found"
	fi
}

case $ACTION in
	"start")
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
