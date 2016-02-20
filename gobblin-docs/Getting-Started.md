Table of Contents
-----------------

[TOC]

# Introduction

This guide will help you setup Gobblin, and run your first job. Currently, Gobblin requires JDK 7 or later to compile and run.

# Download and Build

* Checkout Gobblin:

```bash
git clone https://github.com/linkedin/gobblin.git
```

* Build Gobblin: Gobblin is built using Gradle.

```bash
cd gobblin
./gradlew clean build
```

To build against Hadoop 2, add `-PuseHadoop2`. To skip unit tests, add `-x test`.

# Run Your First Job

Here we illustrate how to run a simple job. This job will pull the five latest revisions of each of the four Wikipedia pages: NASA, Linkedin, Parris_Cues and Barbara_Corcoran. A total of 20 records, each corresponding to one revision, should be pulled if the job is successfully run. The records will be stored as Avro files.

Gobblin can run either in standalone mode or on MapReduce. In this example we will run Gobblin in standalone mode.

This page explains how to run the job from the terminal. You may also run this job from your favorite IDE (IntelliJ is recommended).

## Preliminary 

Each Gobblin job minimally involves several constructs, e.g. [Source](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/Source.java), [Extractor](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/extractor/Extractor.java), [DataWriter](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/writer/DataWriter.java) and [DataPublisher](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/publisher/DataPublisher.java). As the names suggest, Source defines the source to pull data from, Extractor implements the logic to extract data records, DataWriter defines the way the extracted records are output, and DataPublisher publishes the data to the final output location. A job may optionally have one or more Converters, which transform the extracted records, as well as one or more PolicyCheckers that check the quality of the extracted records and determine whether they conform to certain policies.

Some of the classes relevant to this example include [WikipediaSource](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/java/gobblin/example/wikipedia/WikipediaSource.java), [WikipediaExtractor](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/java/gobblin/example/wikipedia/WikipediaExtractor.java), [WikipediaConverter](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/java/gobblin/example/wikipedia/WikipediaConverter.java), [AvroHdfsDataWriter](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/writer/AvroHdfsDataWriter.java) and [BaseDataPublisher](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/publisher/BaseDataPublisher.java).

To run Gobblin in standalone mode we need a Gobblin configuration file (such as uses [gobblin-standalone.properties](https://github.com/linkedin/gobblin/blob/master/conf/gobblin-standalone.properties)). And for each job we wish to run, we also need a job configuration file (such as [wikipedia.pull](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull)). The Gobblin configuration file, which is passed to Gobblin as a command line argument, should contain a property `jobconf.dir` which specifies where the job configuration files are located. By default, `jobconf.dir` points to environment variable `GOBBLIN_JOB_CONFIG_DIR`. Each file in `jobconf.dir` with extension `.job` or `.pull` is considered a job configuration file, and Gobblin will launch a job for each such file. For more information on Gobblin deployment in standalone mode, refer to the [Standalone Deployment](user-guide/Gobblin-Deployment#Standalone-Deployment) page.

A list of commonly used configuration properties can be found here: [Configuration Properties Glossary](user-guide/Configuration-Properties-Glossary).

## Steps

* Create a folder to store the job configuration file. Put [wikipedia.pull](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull) in this folder, and set environment variable `GOBBLIN_JOB_CONFIG_DIR` to point to this folder. Also, make sure that the environment variable `JAVA_HOME` is set correctly.

* Create a folder as Gobblin's working directory. Gobblin will write job output as well as other information there, such as locks and state-store (for more information, see the [Standalone Deployment](user-guide/Gobblin-Deployment#Standalone-Deployment) page). Set environment variable `GOBBLIN_WORK_DIR` to point to that folder.  
<!---stakiar can we list all the folders under gobblin-dist and explain what each folder means -->
* Unpack Gobblin distribution:

```bash
tar -zxvf gobblin-dist-[project-version].tar.gz
cd gobblin-dist
```
* Launch Gobblin:

```bash
bin/gobblin-standalone.sh start
```

This script will launch Gobblin and pass the Gobblin configuration file ([gobblin-standalone.properties](https://github.com/linkedin/gobblin/blob/master/conf/gobblin-standalone.properties)) as an argument.

The job log, which contains the progress and status of the job, will be written into `logs/gobblin-current.log` (to change where the log is written, modify the Log4j configuration file `conf/log4j-standalone.xml`). Stdout will be written into `nohup.out`.

Among the job logs there should be the following information:

```
INFO JobScheduler - Loaded 1 job configuration
INFO  AbstractJobLauncher - Starting job job_PullFromWikipedia_1422040355678
INFO  TaskExecutor - Starting the task executor
INFO  LocalTaskStateTracker2 - Starting the local task state tracker
INFO  AbstractJobLauncher - Submitting task task_PullFromWikipedia_1422040355678_0 to run
INFO  TaskExecutor - Submitting task task_PullFromWikipedia_1422040355678_0
INFO  AbstractJobLauncher - Waiting for submitted tasks of job job_PullFromWikipedia_1422040355678 to complete... to complete...
INFO  AbstractJobLauncher - 1 out of 1 tasks of job job_PullFromWikipedia_1422040355678 are running
INFO  WikipediaExtractor - 5 record(s) retrieved for title NASA
INFO  WikipediaExtractor - 5 record(s) retrieved for title LinkedIn
INFO  WikipediaExtractor - 5 record(s) retrieved for title Parris_Cues
INFO  WikipediaExtractor - 5 record(s) retrieved for title Barbara_Corcoran
INFO  Task - Extracted 20 data records
INFO  Fork-0 - Committing data of branch 0 of task task_PullFromWikipedia_1422040355678_0
INFO  LocalTaskStateTracker2 - Task task_PullFromWikipedia_1422040355678_0 completed in 2334ms with state SUCCESSFUL
INFO  AbstractJobLauncher - All tasks of job job_PullFromWikipedia_1422040355678 have completed
INFO  TaskExecutor - Stopping the task executor 
INFO  LocalTaskStateTracker2 - Stopping the local task state tracker
INFO  AbstractJobLauncher - Publishing job data of job job_PullFromWikipedia_1422040355678 with commit policy COMMIT_ON_FULL_SUCCESS
INFO  AbstractJobLauncher - Persisting job/task states of job job_PullFromWikipedia_1422040355678
```

* After the job is done, stop Gobblin by running

```bash
bin/gobblin-standalone.sh stop
```

The job output is written in `GOBBLIN_WORK_DIR/job-output` folder as an Avro file.

To see the content of the job output, use the Avro tools to convert Avro to JSON. Download the latest version of Avro tools (e.g. avro-tools-1.7.7.jar):

```bash
curl -O http://central.maven.org/maven2/org/apache/avro/avro-tools/1.7.7/avro-tools-1.7.7.jar
```

and run 

```bash
java -jar avro-tools-1.7.7.jar tojson --pretty [job_output].avro > output.json
```

`output.json` will contain all retrieved records in JSON format.

Note that since this job configuration file we used ([wikipedia.pull](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull)) doesn't specify a job schedule, the job will run immediately and will run only once. To schedule a job to run at a certain time and/or repeatedly, set the `job.schedule` property with a cron-based syntax. For example, `job.schedule=0 0/2 * * * ?` will run the job every two minutes. See [this link](http://www.quartz-scheduler.org/documentation/quartz-1.x/tutorials/crontrigger) (Quartz CronTrigger) for more details.


# Other Example Jobs

Besides the Wikipedia example, we have another example job [SimpleJson](https://github.com/linkedin/gobblin/tree/master/gobblin-example/src/main/java/gobblin/example/simplejson), which extracts records from JSON files and store them in Avro files.

To create your own jobs, simply implement the relevant interfaces such as [Source](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/Source.java), [Extractor](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/extractor/Extractor.java), [Converter](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/converter/Converter.java) and [DataWriter](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/writer/DataWriter.java). In the job configuration file, set properties such as `source.class` and `converter.class` to point to these classes.

On a side note: while users are free to directly implement the Extractor interface (e.g., WikipediaExtractor), Gobblin also provides several extractor implementations based on commonly used protocols, e.g., [RestApiExtractor](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/restapi/RestApiExtractor.java), [JdbcExtractor](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/jdbc/JdbcExtractor.java), [SftpExtractor](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/sftp/SftpExtractor.java), etc. Users are encouraged to extend these classes to take advantage of existing implementations.
