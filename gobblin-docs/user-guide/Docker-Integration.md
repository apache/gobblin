# Table of Contents

[TOC]

# Introduction

Gobblin integrates with Docker by running a Gobblin standalone service inside a Docker container. The Gobblin service inside the container can monitor the host filesystem for new job configuration files, run the jobs, and write the resulting data to the host filesystem. The Gobblin Docker images can be found on Docker Hub at: https://hub.docker.com/u/gobblin/

# Docker

For more information on Docker, including how to install it, check out the documentation at: https://docs.docker.com/

# Docker Repositories

Gobblin currently has four different repositories, and all are on Docker Hub [here](https://hub.docker.com/u/gobblin/).

The `gobblin/gobblin-wikipedia` repository contains images that run the Gobblin Wikipedia job found in the [getting started guide](../Getting-Started). These images are useful for users new to Docker or Gobblin, they primarily act as a "Hello World" example for the Gobblin Docker integration.

The `gobblin/gobblin-standalone` repository contains images that run a [Gobblin standalone service](Gobblin-Deployment#standalone-architecture) inside a Docker container. These images provide an easy and simple way to setup a Gobblin standalone service on any Docker compatible machine.

The `gobblin/gobblin-base` and `gobblin/gobblin-distributions` repositories are for internal use only, and are primarily useful for Gobblin developers.

## Gobblin-Wikipedia Repository

The Docker images for this repository can be found on Docker Hub [here](https://hub.docker.com/r/gobblin/gobblin-wikipedia/). These images are mainly meant to act as a "Hello World" example for the Gobblin-Docker integration, and to provide a sanity check to see if the Gobblin-Docker integration is working on a given machine. The image contains the Gobblin configuration files to run the [Gobblin Wikipedia job](../Getting-Started). When a container is launched using the `gobblin-wikipedia` image, Gobblin starts up, runs the Wikipedia example, and then exits.

Running the `gobblin-wikipedia` image requires taking following steps (lets assume we want to an Ubuntu based image):

* Download the images from the `gobblin/gobblin-wikipedia` repository

```
docker pull gobblin/gobblin-wikipedia:ubuntu-gobblin-latest
```

* Run the `gobblin/gobblin-wikipedia:ubuntu-gobblin-latest` image in a Docker container

```
docker run gobblin/gobblin-wikipedia:ubuntu-gobblin-latest
```

The logs are printed to the console, and no errors should pop up. This should provide a nice sanity check to ensure that everything is working as expected. The output of the job will be written to a directory inside the container. When the container exits that data will be lost. In order to preserve the output of the job, continue to the next step.

* Preserving the output of a Docker container requires using a [data volume](https://docs.docker.com/engine/tutorials/dockervolumes/). To do this, run the below command:

```
docker run -v /home/gobblin/work-dir:/home/gobblin/work-dir gobblin-wikipedia
```

The output of the Gobblin-Wikipedia job should now be written to `/home/gobblin/work-dir/job-output`. The `-v` command in Docker uses a feature of Docker called [data volumes](https://docs.docker.com/engine/tutorials/dockervolumes/). The `-v` option mounts a host directory into a container and is of the form `[host-directory]:[container-directory]`. Now any modifications to the host directory can be seen inside the container-directory, and any modifications to the container-directory can be seen inside the host-directory. This is a standard way to ensure data persists even after a Docker container finishes. It's important to note that the `[host-directory]` in the `-v` option can be changed to any directory (on OSX it must be under the `/Users/` directory), but the `[container-directory]` must remain `/home/gobblin/work-dir` (at least for now).

## Gobblin-Standalone Repository

The Docker images for this repository can be found on Docker Hub [here](https://hub.docker.com/r/gobblin/gobblin-standalone/). These images run a Gobblin standalone service inside a Docker container. The Gobblin standalone service is a long running process that can run Gobblin jobs defined in a `.job` or `.pull` file. The job / pull files are submitted to the standalone service by placing them in a directory on the local filesystem. The standalone service monitors this directory for any new job / pull files and runs them either immediately or on a scheduled basis (more information on how this works can be found [here](Working-with-Job-Configuration-Files#adding-or-changing-job-configuration-files)). Running the Gobblin standalone service inside a Docker container allows Gobblin to pick up job / pull files from a directory on the host filesystem, run the job, and write the output back the host filesystem. All the heavy lifting is done inside a Docker container, the user just needs to worry about defining and submitting job / pull files. The goal is to provide a easy to setup environment for the Gobblin standalone service.

Running the `gobblin-standalone` image requires taking the following steps:

* Download the images from the `gobblin/gobblin-standalone` repository

```
docker pull gobblin/gobblin-standalone:ubuntu-gobblin-latest
```

* Run the `gobblin/gobblin-standalone:ubuntu-gobblin-latest` image in a Docker container

```
docker run -v /home/gobblin/conf:/etc/opt/job-conf \
           -v /home/gobblin/work-dir:/home/gobblin/work-dir \
           -v /home/gobblin/logs:/var/log/gobblin \
           gobblin/gobblin-standalone:ubuntu-gobblin-latest
```

A data volume needs to be created for the job configuration directory (contains all the job configuration files), the work directory (contains all the job output data), and the logs directory (contains all the Gobblin standalone logs).

The `-v /home/gobblin/conf:/etc/opt/job-conf` option allows any new job / pull files added to the `/home/gobblin/conf` directory on the host filesystem will be seen by the Gobblin standalone service inside the container. So any job / pull added to the `/home/gobblin/conf` directory on the local filesystem will be run by the Gobblin standalone inside running inside the Docker container. Note the container directory (`/etc/opt/job-conf`) should not be modified, while the host directory (`/home/gobblin/conf`) directory can be any directory on the host filesystem that contains job / pull files.

The `-v /home/gobblin/work-dir:/home/gobblin/work-dir` option allows the container to write data to the host filesystem, so that the data persists after the container is shutdown. Once again, the container directory (`/home/gobblin/work-dir`) should not be modified, while the host directory (`/home/gobblin/work-dir`) can be any directory on the host filesystem.

The `-v /home/gobblin/logs:/var/log/gobblin` option allows the Gobblin standalone logs to be written to the host filesystem, so that they can be read on the host machine. This is useful for monitoring and debugging purposes. Once again, the container directory (`/var/log/gobblin`) directory should not be modified, while the container directory (`/home/gobblin/logs`) can be any directory on the host filesystem.

# Future Work

* Create `gobblin-dev` images that provide an development environment for Gobblin contributors
* Create `gobblin-kafka` images that provide an end-to-end service for writing to Kafka and ingesting the Kafka data through Gobblin
* Test and write a tutorial on using `gobblin-standalone` images to write to a HDFS cluster
* Create images based on [Linux Alpine](https://hub.docker.com/_/alpine/) (lightweight Linux distro)
