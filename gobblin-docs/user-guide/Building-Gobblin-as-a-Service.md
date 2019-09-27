Table of Contents
--------------------

[TOC]


# Introduction
Gobblin as a service is a service that takes in a user request (a logical flow) and converts it into a series of Gobblin Jobs, and monitors these jobs in a distributed manner.
The design of the service can be found here: https://cwiki.apache.org/confluence/display/GOBBLIN/Gobblin+as+a+Service

# Running Gobblin as a Service
1. [Build Gobblin] (./Building-Gobblin.md) or use one of the [provided distributions] (https://github.com/apache/incubator-gobblin/releases)
2. Untar the build file `tar -xvf apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz`
3. Execute the start script `./gobblin-dist/bin/gobblin-service.sh`
4. View output in `service.out`

Currently the setup only runs a portion of the service, but work will be done to have a basic end-to-end workflow soon.

The service can now be accessed on `localhost:6956`

# Running Gobblin as a Service with Docker
There is also a Dockerfile to create new images of Gobblin based on the source code that can be easily run.

1. `export GOBBLIN_WORK_DIR=<root_directory_of_gobblin>`
2. `docker compose -f gobblin-docker/gobblin-service/alpine-gaas-latest/docker-compose.yml build`
3. `docker compose -f gobblin-docker/gobblin-service/alpine-gaas-latest/docker-compose.yml up`
 
The docker container exposes the endpoints which can be accessed on `localhost:6956`