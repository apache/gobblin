#!/bin/bash

usage="Usage: sh uif-daemon.sh (start | status | restart | stop | byteman) [-p | --props <props-file-location>] [-cp | --classpath <java-classpath>]"

start_stop=$1
shift

while [ "$1" != "" ]; do
	case $1 in
		-h | --help)
			echo $usage
			exit
			;;
		-p | --props)
			shift
			props=$1
			;;
		-cp | --classpath)
			shift
			classpathLoc=$1
			;;
		*)
			echo $usage
			exit 1
	esac
	shift
done

pid=uif-pid.txt

if [ -f $pid ]; then
	pid_value=`cat $pid` > /dev/null 2>&1
else
	pid_value=""
fi

uif_stop_timeout=1

status() {
	if [ -f $pid ]; then
		echo "Looking into file: $pid"
		if kill -0 $pid_value > /dev/null 2>&1; then
			echo "UIF is running with status:"
			ps -ef | grep -v grep | grep $pid_value
		else
			echo "UIF is not running"
		fi
	else
		echo "No pid file found"
	fi		
}

start() {
	if [ -z $props ]; then
		echo "Props is not set"
		echo $usage
		exit 1
	fi
	if [ -z $classpathLoc ]; then
		echo "Classpath is not set"
		echo $usage
		exit 1
	fi

	classpath=$(find $classpathLoc | tr $'\012' ':' | sed 's/:$//g')
	command="java -cp $classpath com.linkedin.uif.scheduler.Worker $props"

	if [ -f $pid ]; then
		if kill -0 $pid_value > /dev/null 2>&1; then
			echo 'UIF is already running, stop it first'
			exit 1
		fi
	fi
	echo 'Starting UIF'
	nohup $command > uif.log & echo $! > $pid
}

stop() {
	if [ -f $pid ]; then
		if kill -0 $pid_value > /dev/null 2>&1; then
			echo 'Stopping UIF'
			kill $pid_value
			sleep $uif_stop_timeout
			if kill -0 $pid_value > /dev/null 2>&1; then
				echo "UIF did not stop gracefully, killing with kill -9"
				kill -9 $pid_value
			fi
		else
			echo "Process $pid_value is not running"
		fi
	else
		echo "No pid file found"
	fi
}

byteman() {
	export BYTEMAN_HOME=$(pwd)/test/byteman/byteman-download-2.1.4.1
	if [ ! -e "test/byteman/byteman-download-2.1.4.1" ]; then
		cd test/byteman
        	curl -OL http://downloads.jboss.org/byteman/2.1.4.1/byteman-download-2.1.4.1-full.zip
        	unzip byteman-download-2.1.4.1-full.zip
        	cd ../../
    	fi
	byteman_param="-javaagent:${BYTEMAN_HOME}/lib/byteman.jar=script:test/byteman/negative_test.btm"
}

case $start_stop in
	"status")
		status
		;;
	"start")
		start
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
	"byteman")
		byteman
		;;
	*)
		echo $usage
		exit 1
		;;
esac
