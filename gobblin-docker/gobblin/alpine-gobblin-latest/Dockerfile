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

FROM gradle:5.6.4-jdk8 as build
WORKDIR /home/gobblin
ARG GOBBLIN_FLAVOR=standard

# copy the entire project folder
COPY . .
RUN gradle build -x findbugsMain -x test -x rat -x checkstyleMain -x javadoc -x checkstyleTest -PgobblinFlavor=${GOBBLIN_FLAVOR} --no-daemon && \
    tar -xvf ./build/gobblin-distribution/distributions/*.tar.gz && \
    rm *.tar.gz

FROM openjdk:8-jre-alpine
WORKDIR /home/gobblin/
COPY ./gobblin-docker/gobblin/alpine-gobblin-latest/entrypoint.sh ./bin/entrypoint.sh
COPY --from=build /home/gobblin/gobblin-dist .
RUN apk add --no-cache bash && \
    mkdir /tmp/gobblin && \
    mkdir /etc/gobblin && \
    mkdir /etc/gobblin/jobs && \
    chmod +x ./bin/entrypoint.sh

ENV GOBBLIN_WORK_DIR=/tmp/gobblin/
ENV GOBBLIN_JOB_CONFIG_DIR=/etc/gobblin/jobs

ENTRYPOINT ["./bin/entrypoint.sh"]