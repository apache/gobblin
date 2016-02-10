#!/bin/bash

echo "CLEANING"
./gradlew clean

for P in :gobblin-admin :gobblin-api :gobblin-azkaban :gobblin-compaction :gobblin-config-management:gobblin-config-core :gobblin-config-management:gobblin-config-client :gobblin-core :gobblin-data-management :gobblin-distribution :gobblin-example :gobblin-metastore :gobblin-metrics :gobblin-oozie :gobblin-rest-service:gobblin-rest-api :gobblin-rest-service:gobblin-rest-client :gobblin-rest-service:gobblin-rest-server :gobblin-runtime :gobblin-salesforce :gobblin-scheduler :gobblin-test :gobblin-test-harness :gobblin-utility :gobblin-yarn ; do 
    echo "----------------------------" 
    read -p "UPLOAD $P (y/n)" ans 
    if [ "$ans" == "y" -o "$ans" == "Y" -o "$ans" == "yes" -o "$ans" == "Yes" -o "$ans" == "YES" ] ; then
        ./maven-sonatype.sh -remote -hadoop1 -noclean -packages $P
    else
        echo "Skipping $P"
    fi
done

