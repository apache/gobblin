#!/bin/bash

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

PROG=$(basename $0)

function usage() {
    echo -e "USAGE: $PROG"
}

# main()

if [ "$#" -eq 0 ] ; then
    usage
    exit
fi

while [ "$#" -gt 0 ] ; do
    A="$1"
    case "$A" in
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
       ./maven-nexus/maven-nexus.sh -remote -noclean -packages $P
    else
       echo "Skipping $P"
    fi
 done

