Table of Contents
-----------------

[TOC]

# Introduction

This guide will help you setup Gobblin, and run your first job. Currently, Gobblin requires JDK 7 or later to run.

# Getting a Gobblin Distribution

All steps in this page assume you are using a Gobblin distribution. A distribution contains the Gobblin binaries in a specific directory structure, and is different from the structure of the repository. 

Distributions are generated and distributed as `*.tar.gz` files. After getting the tarball, unpackage it locally:

`tar -xvf gobblin-distribution-[VERSION].tar.gz`.

There are two way to obtain a distribution tarball

## Downloading a Pre-Built Distribution

Download the latest Gobblin release from the [Release Page](https://github.com/linkedin/gobblin/releases). You will want to download the `gobblin-distribution-[RELEASE-VERSION].tar.gz` file.

## Building a Distribution

Clone the repository into your system. 

Build a distribution:

```bash
cd /path/to/gobblin/repo
./gradlew :gobblin-distribution:buildDistributionTar
```

Note: A full build takes time because it runs other tasks like test, javadoc, findMainBugs, etc, which impacts the build performance. 
For a quick usage, building distribution is good enough. However a full build can be easily made by running:
```bash
./gradlew build
```

After the build is done, there should be a tarball (if there are multiple, use the newest one) at 

`build/gobblin-distribution/distributions/`

# Run Your First Job

Note: the following two sections are only applicable to newer versions of Gobblin. If you are running version 0.8.0 or earlier, skip to [Gobblin daemon](#running-gobblin-as-a-daemon).

Here we illustrate how to run a simple job. This job will pull revisions for the last ten days of each of the two Wikipedia pages: Linkedin, Wikipedia:Sandbox (a page with frequent edits). The records will be written to stdout.

Gobblin can run either in standalone mode or on MapReduce. In this example we will run Gobblin in standalone mode.

This page explains how to run the job from the terminal. You may also run this job from your favorite IDE (IntelliJ is recommended).

## Steps

* cd to the unpacked Gobblin distribution and run `bin/gobblin run` to get usage.
* Running `bin/gobblin run listQuickApps` will list the available easy-to-configure apps. Note the line with the wikipedia example:
```bash
wikipedia	-	Gobblin example that downloads revisions from Wikipedia.
```
* Running `bin/gobblin run wikipedia` will show the usage of this application. Notice the usage and one of the options listed for this job:
```bash
usage: gobblin run wikipedia [OPTIONS] <article-title> [<article-title>...]
 -lookback <arg>             Sets the period for which articles should be
                             pulled in ISO time format (e.g. P2D, PT1H)
```
* Run `bin/gobblin run wikipedia -lookback P10D LinkedIn Wikipedia:Sandbox`. This will print a lot of logs, but somewhere in there you will see a few json entries with the revisions for those articles. For example:
```bash
{"revid":746260034,"parentid":745444076,"user":"2605:8D80:580:5824:B108:82BD:693D:CFA1","anon":"","userid":0,"timestamp":"2016-10-26T08:12:09Z","size":69527,"pageid":970755,"title":"LinkedIn"}
```
* In the usage, there is also an option to instead write the output to an avro file:
```bash
 -avroOutput <arg>           Write output to Avro files. Specify the
                             output directory as argument.
```
Running `bin/gobblin run wikipedia -lookback P10D -avroOutput /tmp/wikiSample LinkedIn Wikipedia:Sandbox` will create a directory `/tmp/wikiSample` with two subdirectories `LinkedIn` and `Wikipedia_Sandbox` each one with one avro file.

# Running Gobblin as a Daemon

Here we show how to run a Gobblin daemon. A Gobblin daemon tracks a directory and finds job configuration files in it (jobs with extensions `*.pull`). Job files can be either run once or scheduled jobs. Gobblin will automatically execute this jobs as they are received following the schedule.

For this example, we will once again run the Wikipedia example. The records will be stored as Avro files.

## Preliminary

Each Gobblin job minimally involves several constructs, e.g. [Source](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/Source.java), [Extractor](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/extractor/Extractor.java), [DataWriter](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/writer/DataWriter.java) and [DataPublisher](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/publisher/DataPublisher.java). As the names suggest, Source defines the source to pull data from, Extractor implements the logic to extract data records, DataWriter defines the way the extracted records are output, and DataPublisher publishes the data to the final output location. A job may optionally have one or more Converters, which transform the extracted records, as well as one or more PolicyCheckers that check the quality of the extracted records and determine whether they conform to certain policies.

Some of the classes relevant to this example include [WikipediaSource](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/java/gobblin/example/wikipedia/WikipediaSource.java), [WikipediaExtractor](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/java/gobblin/example/wikipedia/WikipediaExtractor.java), [WikipediaConverter](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/java/gobblin/example/wikipedia/WikipediaConverter.java), [AvroHdfsDataWriter](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/writer/AvroHdfsDataWriter.java) and [BaseDataPublisher](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/publisher/BaseDataPublisher.java).

To run Gobblin in standalone daemon mode we need a Gobblin configuration file (such as uses [gobblin-standalone.properties](https://github.com/linkedin/gobblin/blob/master/conf/gobblin-standalone.properties)). And for each job we wish to run, we also need a job configuration file (such as [wikipedia.pull](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull)). The Gobblin configuration file, which is passed to Gobblin as a command line argument, should contain a property `jobconf.dir` which specifies where the job configuration files are located. By default, `jobconf.dir` points to environment variable `GOBBLIN_JOB_CONFIG_DIR`. Each file in `jobconf.dir` with extension `.job` or `.pull` is considered a job configuration file, and Gobblin will launch a job for each such file. For more information on Gobblin deployment in standalone mode, refer to the [Standalone Deployment](user-guide/Gobblin-Deployment#Standalone-Deployment) page.

A list of commonly used configuration properties can be found here: [Configuration Properties Glossary](user-guide/Configuration-Properties-Glossary).

## Steps

* Create a folder to store the job configuration file. Put [wikipedia.pull](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull) in this folder, and set environment variable `GOBBLIN_JOB_CONFIG_DIR` to point to this folder. Also, make sure that the environment variable `JAVA_HOME` is set correctly.

* Create a folder as Gobblin's working directory. Gobblin will write job output as well as other information there, such as locks and state-store (for more information, see the [Standalone Deployment](user-guide/Gobblin-Deployment#Standalone-Deployment) page). Set environment variable `GOBBLIN_WORK_DIR` to point to that folder.

* Unpack Gobblin distribution:

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

To see the content of the job output, use the Avro tools to convert Avro to JSON. Download the latest version of Avro tools (e.g. avro-tools-1.8.1.jar):

```bash
curl -O http://central.maven.org/maven2/org/apache/avro/avro-tools/1.8.1/avro-tools-1.8.1.jar
```

and run

```bash
java -jar avro-tools-1.8.1.jar tojson --pretty [job_output].avro > output.json
```

`output.json` will contain all retrieved records in JSON format.

Note that since this job configuration file we used ([wikipedia.pull](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull)) doesn't specify a job schedule, the job will run immediately and will run only once. To schedule a job to run at a certain time and/or repeatedly, set the `job.schedule` property with a cron-based syntax. For example, `job.schedule=0 0/2 * * * ?` will run the job every two minutes. See [this link](http://www.quartz-scheduler.org/documentation/quartz-2.1.x/tutorials/crontrigger.html) (Quartz CronTrigger) for more details.

# Other Example Jobs

Besides the Wikipedia example, we have another example job [SimpleJson](https://github.com/linkedin/gobblin/tree/master/gobblin-example/src/main/java/gobblin/example/simplejson), which extracts records from JSON files and store them in Avro files.

To create your own jobs, simply implement the relevant interfaces such as [Source](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/Source.java), [Extractor](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/extractor/Extractor.java), [Converter](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/converter/Converter.java) and [DataWriter](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/writer/DataWriter.java). In the job configuration file, set properties such as `source.class` and `converter.class` to point to these classes.

On a side note: while users are free to directly implement the Extractor interface (e.g., WikipediaExtractor), Gobblin also provides several extractor implementations based on commonly used protocols, e.g., [KafkaExtractor](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/KafkaExtractor.java), [RestApiExtractor](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/restapi/RestApiExtractor.java), [JdbcExtractor](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/jdbc/JdbcExtractor.java), [SftpExtractor](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/sftp/SftpExtractor.java), etc. Users are encouraged to extend these classes to take advantage of existing implementations.
