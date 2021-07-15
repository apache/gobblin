# Table of Contents

[TOC]

# Introduction

Gobblin integrates with Docker by running a Gobblin standalone service inside a Docker container. The Gobblin service inside the container can monitor the host filesystem for new job configuration files, run the jobs, and write the resulting data to the host filesystem. The Gobblin Docker images can be found on Docker Hub at: https://hub.docker.com/r/apache/gobblin

# Docker

For more information on Docker, including how to install it, check out the documentation at: https://docs.docker.com/

# Docker Repositories

Github Actions pushes the latest docker image to the Apache DockerHub repository [here](https://hub.docker.com/r/apache/gobblin) from `gobblin-docker/gobblin/alpine-gobblin-latest/Dockerfile`

To run this image, you will need to pass in the corresponding execution mode. The execution modes can be found [here](https://gobblin.readthedocs.io/en/latest/user-guide/Gobblin-Deployment/)

```
docker pull apache/gobblin
docker run apache/gobblin --mode <execution mode> <additional args>
```

For example, to run Gobblin in standalone mode
```
docker run apache/gobblin --mode standalone
```

To pass your own configuration to Gobblin standalone, use a docker volume. Due to the nature of the startup script, the volumes
will need to be declared before the arguments are passed to the execution mode. E.g.
```
docker run -v <path to local configuration files>:/home/gobblin/conf/standalone apache/gobblin --mode standalone
```

Before running docker containers, set a working directory for Gobblin jobs:

`export LOCAL_JOB_DIR=<local_gobblin_directory>`

We will use this directory as the [volume](https://docs.docker.com/storage/volumes/) for Gobblin jobs and outputs. Make sure your Docker has the [access](https://docs.docker.com/docker-for-mac/#file-sharing) to this folder. This is the prerequisite for all following example jobs.

### Run the docker image with simple wikipedia jobs

Run these commands to start the docker image:

`docker pull apache/gobblin:latest`

`docker run -v $LOCAL_JOB_DIR:/etc/gobblin-standalone/jobs apache/gobblin:latest --mode standalone`

After the container spins up, put the [wikipedia.pull](https://github.com/apache/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull) in ${LOCAL_JOB_DIR}. You will see the Gobblin daemon pick up the job, and the result output is in ${LOCAL_JOB_DIR}/job-output/.

This example job is correspondent to the [getting started guide](https://gobblin.readthedocs.io/en/latest/Getting-Started/). With the docker image, you can focus on the Gobblin functionalities, avoiding the hassle of building a distribution.

### Use Gobblin Standalone on Docker for Kafka and HDFS Ingestion 

* To ingest from/to Kafka and HDFS by Gobblin, you need to start services for Zookeeper, Kafka and HDFS along with Gobblin. We use docker [compose](https://docs.docker.com/compose/) with images contributed to docker hub. Firstly, you need to create a [docker-compose.yml](https://github.com/apache/gobblin/blob/master/gobblin-docker/gobblin-recipes/kafka-hdfs/docker-compose.yml) file.

* Second, in the same folder of the yml file, create a [hadoop.env](https://github.com/apache/gobblin/blob/master/gobblin-docker/gobblin-recipes/kafka-hdfs/hadoop.env) file to specify all HDFS related config(copy the content into your .env file).

* Open a terminal in the same folder, pull and run these docker services:

    `docker-compose -f ./docker-compose.yml pull`

    `docker-compose -f ./docker-compose.yml up`
    
    Here we expose Zookeeper at port 2128, Kafka at 9092 with an auto created Kafka topic “test”. All hadoop related configs are stated in the .env file.

* You should see all services running. Now we can push some events into the Kafka topic. Open a terminal from [docker desktop](https://docs.docker.com/desktop/dashboard/) dashboard or [docker exec](https://docs.docker.com/engine/reference/commandline/exec/) to interact with Kafka. Inside the Kafka container terminal:

    `cd /opt/kafka`

    `./bin/kafka-console-producer.sh --broker-list kafka:9092 --topic test`

    You can type messages for the topic “test”, and press ctrl+c to exit.

* Put the [kafka-hdfs.pull](https://github.com/apache/gobblin/blob/master/gobblin-example/src/main/resources/kafka-hdfs.pull) in ${LOCAL_JOB_DIR}, so that the Gobblin daemon will pick up this job and write the result to HDFS. You will see the Gobblin daemon pick up the job.

After the job finished, open a terminal in the HDFS namenode container:

`hadoop fs -ls /gobblintest/job-output/test/`

You will see the result file in this HDFS folder. You can use this command to verify the content in the text file:

`hadoop fs -cat /gobblintest/job-output/test/<output_file.txt>`

# Run Gobblin as a Service

The goal of GaaS(Gobblin as a Service) is to enable a self service so that different users can automatically provision and execute various supported Gobblin applications limiting the need for development and operation teams to be involved during the provisioning process. You can take a look at our [design detail](https://cwiki.apache.org/confluence/display/GOBBLIN/Gobblin+as+a+Service).

### Set working directory

Similar to standalone working directory settings:

`export GAAS_JOB_DIR=<gaas_gobblin_directory>`

`export LOCAL_DATAPACK_DIR=<local_directory_of_templateUris>`

### Start Gobblin as a Service

Run these commands to start the docker image:

`docker run -p 6956:6956 -v $GAAS_JOB_DIR:/etc/gobblin-as-service/jobs -v $LOCAL_DATAPACK_DIR:/etc/templateCatalog apache/gobblin --mode gobblin-as-service`

The GaaS will be started, and the service can now be accessed on localhost:6956.


# Future Work

* Complete `gobblin-service` docker guidance that serve as a quick-start for GaaS user
* Implement a simple converter and inject into the docker service. Create a corresponding doc to guide users implement their own logic but no need to tangle with the Gobblin codebase
* Finish the Github action to automate the docker build
