# Table of Contents

[TOC]

# Introduction

Gobblin integrates with Docker by running a Gobblin standalone service inside a Docker container. The Gobblin service inside the container can monitor the host filesystem for new job configuration files, run the jobs, and write the resulting data to the host filesystem. The Gobblin Docker images can be found on Docker Hub at: https://hub.docker.com/u/gobblin/

# Docker

For more information on Docker, including how to install it, check out the documentation at: https://docs.docker.com/

# Docker Repositories

Gobblin currently has four different repositories, and all are on Docker Hub [here](https://hub.docker.com/u/gobblin/). We are also starting to use [Apache's repository](https://hub.docker.com/r/apache/gobblin/tags?page=1&ordering=last_updated) for our images. 

The `gobblin/gobblin-wikipedia` repository contains images that run the Gobblin Wikipedia job found in the [getting started guide](../Getting-Started). These images are useful for users new to Docker or Gobblin, they primarily act as a "Hello World" example for the Gobblin Docker integration.

The `gobblin/gobblin-standalone` repository contains images that run a [Gobblin standalone service](Gobblin-Deployment#standalone-architecture) inside a Docker container. These images provide an easy and simple way to setup a Gobblin standalone service on any Docker compatible machine.

The `gobblin/gobblin-service` repository contains images that run [Gobblin as a service](Building-Gobblin-as-a-Service#running-gobblin-as-a-service-with-docker), which is a service that takes in a user request (a logical flow) and converts it into a series of Gobblin Jobs, and monitors these jobs in a distributed manner.

The `gobblin/gobblin-base` and `gobblin/gobblin-distributions` repositories are for internal use only, and are primarily useful for Gobblin developers.

# Run Gobblin Standalone

The Docker images for this repository can be found on Docker Hub [here](https://hub.docker.com/r/gobblin/gobblin-standalone/). These images run a Gobblin standalone service inside a Docker container. The Gobblin standalone service is a long running process that can run Gobblin jobs defined in a `.job` or `.pull` file. The job / pull files are submitted to the standalone service by placing them in a directory on the local filesystem. The standalone service monitors this directory for any new job / pull files and runs them either immediately or on a scheduled basis (more information on how this works can be found [here](Working-with-Job-Configuration-Files#adding-or-changing-job-configuration-files)). Running the Gobblin standalone service inside a Docker container allows Gobblin to pick up job / pull files from a directory on the host filesystem, run the job, and write the output back the host filesystem. All the heavy lifting is done inside a Docker container, the user just needs to worry about defining and submitting job / pull files. The goal is to provide a easy to setup environment for the Gobblin standalone service.

### Set working directory

Before running docker containers, set a working directory for Gobblin jobs:

`export LOCAL_JOB_DIR=<local_gobblin_directory>`

We will use this directory as the [volume](https://docs.docker.com/storage/volumes/) for Gobblin jobs and outputs. Make sure your Docker has the [access](https://docs.docker.com/docker-for-mac/#file-sharing) to this folder. This is the prerequisite for all following example jobs.

### Run the docker image with simple wikipedia jobs

Run these commands to start the docker image:

`docker pull apache/gobblin:latest`

`docker run -v $LOCAL_JOB_DIR:/tmp/gobblin-standalone/jobs apache/gobblin:latest`

After the container spins up, put the [wikipedia.pull](https://github.com/apache/incubator-gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull) in ${LOCAL_JOB_DIR}. You will see the Gobblin daemon pick up the job, and the result output is in ${LOCAL_JOB_DIR}/job-output/.

This example job is correspondent to the [getting started guide](https://gobblin.readthedocs.io/en/latest/Getting-Started/). With the docker image, you can focus on the Gobblin functionalities, avoiding the hassle of building a distribution.

### Use Gobblin Standalone on Docker for Kafka and HDFS Ingestion 

* To ingest from/to Kafka and HDFS by Gobblin, you need to start services for Zookeeper, Kafka and HDFS along with Gobblin. We use docker [compose](https://docs.docker.com/compose/) with images contributed to docker hub. Firstly, you need to create a [docker-compose.yml](https://github.com/apache/incubator-gobblin/blob/master/gobblin-docker/gobblin-recipes/kafka-hdfs/docker-compose.yml) file.

* Second, in the same folder of the yml file, create a [hadoop.env](https://github.com/apache/incubator-gobblin/blob/master/gobblin-docker/gobblin-recipes/kafka-hdfs/hadoop.env) file to specify all HDFS related config(copy the content into your .env file).

* Open a terminal in the same folder, pull and run these docker services:

    `docker-compose -f ./docker-compose.yml pull`

    `docker-compose -f ./docker-compose.yml up`
    
    Here we expose Zookeeper at port 2128, Kafka at 9092 with an auto created Kafka topic “test”. All hadoop related configs are stated in the .env file.

* You should see all services running. Now we can push some events into the Kafka topic. Open a terminal from [docker desktop](https://docs.docker.com/desktop/dashboard/) dashboard or [docker exec](https://docs.docker.com/engine/reference/commandline/exec/) to interact with Kafka. Inside the Kafka container terminal:

    `cd /opt/kafka`

    `./bin/kafka-console-producer.sh --broker-list kafka:9092 --topic test`

    You can type messages for the topic “test”, and press ctrl+c to exit.

* Put the [kafka-hdfs.pull](https://github.com/apache/incubator-gobblin/blob/master/gobblin-example/src/main/resources/kafka-hdfs.pull) in ${LOCAL_JOB_DIR}, so that the Gobblin daemon will pick up this job and write the result to HDFS. You will see the Gobblin daemon pick up the job.

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

`docker pull gobblin/gobblin-service:alpine-gaas-latest`

`docker run -p 6956:6956 -v GAAS_JOB_DIR:/tmp/gobblin-as-service/jobs -v LOCAL_DATAPACK_DIR:/tmp/templateCatalog gobblin/gobblin-service:alpine-gaas-latest`

The GaaS will be started, and the service can now be accessed on localhost:6956.

### Interact with GaaS

##### TODO: Add an end-to-end workflow example in GaaS.

# Future Work

* Complete `gobblin-service` docker guidance that serve as a quick-start for GaaS user
* Implement a simple converter and inject into the docker service. Create a corresponding doc to guide users implement their own logic but no need to tangle with the Gobblin codebase
* Finish the Github action to automate the docker build
