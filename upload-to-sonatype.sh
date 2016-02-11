#!/bin/bash

PROG=$(basename $0)

function usage() {
    echo -e "USAGE: $PROG -hadoop1|-hadoop2"
}

# main()

if [ "$#" -eq 0 ] ; then
    usage
    exit
fi

hadoop_version_flag=

while [ "$#" -gt 0 ] ; do
    A="$1"
    case "$A" in
        -hadoop1)
            hadoop_version_flag=$A
            ;;
        -hadoop2)
            hadoop_version_flag=
            ;;
        -h|--help)
            usage
            exit
            ;;
        *)
            echo "$PROG: unknown option: $A"
            exit 1
            ;;
    esac
    shift
done 

echo "CLEANING"
./gradlew clean

upload_all=0

for P in :gobblin-admin :gobblin-api :gobblin-azkaban :gobblin-compaction :gobblin-config-management:gobblin-config-core :gobblin-config-management:gobblin-config-client :gobblin-core :gobblin-data-management :gobblin-distribution :gobblin-example :gobblin-metastore :gobblin-metrics :gobblin-oozie :gobblin-rest-service:gobblin-rest-api :gobblin-rest-service:gobblin-rest-client :gobblin-rest-service:gobblin-rest-server :gobblin-runtime :gobblin-salesforce :gobblin-scheduler :gobblin-test :gobblin-test-harness :gobblin-utility :gobblin-yarn ; do 
    echo "----------------------------" 
    if [ "$upload_all" -eq 0 ] ; then
        read -p "UPLOAD $P [y(es)|n(o)|a(ll)]" ans
        ans=$(echo "$ans" | tr '[:upper:]' '[:lower:]') 
        if [ "$ans" == "a" -o "$ans" == "all" ] ; then
            upload_all=1
            ans="y"
        fi
   else
        ans="y" 
    fi
    if [ "$ans" == "y" -o "$ans" == "yes" ] ; then
       ./maven-sonatype.sh ${hadoop_version_flag} -remote -noclean -packages $P
    else
       echo "Skipping $P"
    fi
 done

