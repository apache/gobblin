#!/bin/bash

usage="Usage: sh log-extract.sh [-f | --file <location of the log file>] [-t | --tableName <format SOURCESCHEMA_SOURCEENTITY>] [-w | --workunitName <WorkUnit name>]"

while [ "$1" != "" ]; do
        case $1 in
                -h | --help)
                        echo $usage
                        exit
                        ;;
                -t | --tableName)
                        shift
                        tableName=$1
                        ;;
                -w | --workunitName)
                        shift
                        workunitName=$1
                        ;;
		-f | --file)
			shift
			fileLocation=$1
			;;
                *)
                        echo $usage
                        exit 1
        esac
        shift
done

if [ -z $fileLocation ]; then
	echo "Must specify log file location"
	echo $usage
fi

if [ -z $tableName ]; then
	echo "Must specify table name"
	echo $usage
fi

if [ -f $fileLocation ]; then
	echo $fileLocation
	while IFS= read -r line
	do
		if [[ $line == *$tableName* ]]; then
			echo "$line"
		fi
	done < "$fileLocation"
else
	echo "Specified file location does not exist"
	echo $usage
fi
