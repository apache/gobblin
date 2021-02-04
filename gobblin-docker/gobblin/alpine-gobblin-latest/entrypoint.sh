#!/bin/bash

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
# Execution Modes

STANDALONE_MODE='standalone'
CLUSTER_MASTER_MODE='cluster-master'
CLUSTER_WORKER_MODE='cluster-worker'
AWS_MODE='aws'
YARN_MODE='yarn'
MAPREDUCE_MODE='mapreduce'
GOBBLIN_AS_SERVICE_MODE='gobblin-as-service'

GOBBLIN_EXEC_MODE_LIST="$STANDALONE_MODE, $CLUSTER_MASTER_MODE, $CLUSTER_WORKER_MODE, $AWS_MODE, $YARN_MODE, $MAPREDUCE_MODE, $GOBBLIN_AS_SERVICE_MODE"
GOBBLIN_HOME="$(cd `dirname $0`/..; pwd)"

EXECUTION_MODE=''
CONF_PATH=''
JVM_OPTS=''
ARGS=("$@")

shopt -s nocasematch
for i in "${!ARGS[@]}"
  do
    case "${ARGS[$i]}" in
      --mode )
        EXECUTION_MODE="${ARGS[$i+1]}"
      ;;
  esac
done

if [[ -z "$EXECUTION_MODE" ]]; then
  echo "Execution mode missing: expected one of the following: $GOBBLIN_EXEC_MODE_LIST"
  echo "Usage: docker run <docker_image_tag> --mode <execution_mode> <additional_options>"
  exit 1
fi

./bin/gobblin.sh service $EXECUTION_MODE start --log-to-stdout "$@"
