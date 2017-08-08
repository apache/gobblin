# Table of Contents

[TOC]

# Introduction

Gobblin jobs can be scheduled on a recurring basis using a few different tools. Gobblin ships with a built in [Quartz Scheduler](https://quartz-scheduler.org/). Gobblin also integrates with a few other third party tools.

# Quartz

Gobblin has a built in Quartz scheduler as part of the [`JobScheduler`](https://github.com/linkedin/gobblin/blob/master/gobblin-scheduler/src/main/java/gobblin/scheduler/JobScheduler.java) class. This class integrates with the Gobblin [`SchedulerDaemon`](https://github.com/linkedin/gobblin/blob/master/gobblin-scheduler/src/main/java/gobblin/scheduler/SchedulerDaemon.java), which can be run using the Gobblin [`bin/gobblin-standalone.sh](https://github.com/linkedin/gobblin/blob/master/bin/gobblin-standalone.sh) script.

So in order to take advantage of the Quartz scheduler two steps need to be taken:

* Use the `bin/gobblin-standalone.sh` script
* Add the property `job.schedule` to the `.pull` file
    * The value for this property should be a [CRONTrigger](http://quartz-scheduler.org/api/2.2.0/org/quartz/CronTrigger.html)

# Azkaban

Gobblin can be launched via [Azkaban](https://azkaban.github.io/), and open-source Workflow Manager for scheduling and launching Hadoop jobs. Gobblin's [`AzkabanJobLauncher`](https://github.com/linkedin/gobblin/blob/master/gobblin-azkaban/src/main/java/gobblin/azkaban/AzkabanJobLauncher.java) can be used to launch a Gobblin job through Azkaban.

One has to follow the typical setup to create a zip file that can be uploaded to Azkaban (it should include all dependent jars, which can be found in `gobblin-dist.tar.gz`). The `.job` file for the Azkaban Job should contain all configuration properties that would be put in a `.pull` file (for example, the [Wikipedia Example](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull) `.pull` file). All Gobblin system dependent properties (e.g. [`conf/gobblin-mapreduce.properties`](https://github.com/linkedin/gobblin/blob/master/conf/gobblin-mapreduce.properties) or [`conf/gobblin-standalone.properties`](https://github.com/linkedin/gobblin/blob/master/conf/gobblin-standalone.properties)) should also be in the zip file.

In the Azkaban `.job` file, the `type` parameter should be set to `hadoopJava` (see [here](http://azkaban.github.io/azkaban/docs/latest/#hadoopjava-type) for more information about the `hadoopJava` Job Type). The `job.class` parameter should be set to `gobblin.azkaban.AzkabanJobLauncher`.

# Oozie

[Oozie](https://oozie.apache.org/) is a very popular scheduler for the Hadoop environment. It allows users to define complex workflows using XML files. A workflow can be composed of a series of actions, such as Java Jobs, Pig Jobs, Spark Jobs, etc. Gobblin has two integration points with Oozie. It can be run as a stand-alone Java process via Oozie's `<java>` tag, or it can be run as an Map Reduce job via Oozie.

The following guides assume Oozie is already setup and running on some machine, if this is not the case consult the Oozie documentation for getting everything setup.

These tutorial only outline how to launch a basic Oozie job that simply runs a Gobblin java a single time. For information on how to build more complex flows, and how to run jobs on a schedule, check out the Oozie documentation online.

### Launching Gobblin in Local Mode

This guide focuses on getting Gobblin to run in as a stand alone Java Process. This means it will not launch a separate MR job to distribute its workload. It is important to understand how the current version of Oozie will launch a Java process. It will first start an MapReduce job and will run the Gobblin as a Java process inside a single map task. The Gobblin job will then ingest all data it is configured to pull and then it will shutdown.

#### Example Config Files

[`gobblin-oozie/src/main/resources/local`](https://github.com/linkedin/gobblin/tree/master/gobblin-oozie/src/main/resources/local) contains sample configuration files for launching Gobblin Oozie. There are a number of important files in this directory:

`gobblin-oozie-example-system.properties` contains default system level properties for Gobblin. When launched with Oozie, Gobblin will run inside a map task; it is thus recommended to configure Gobblin to write directly to HDFS rather than the local file system. The property `fs.uri` in this file should be changed to point to the NameNode of the Hadoop File System the job should write to. By default, all data is written under a folder called `gobblin-out`; to change this modify the `gobblin.work.dir` parameter in this file.

`gobblin-oozie-example-workflow.properties` contains default Oozie properties for any job launched. It is also the entry point for launching an Oozie job (e.g. to launch an Oozie job from the command line you execute `oozie job -config gobblin-oozie-example-workflow.properties -run`). In this file one needs to update the `name.node` and `resource.manager` to the values specific to their environment. Another important property in this file is `oozie.wf.application.path`; it points to a folder on HDFS that contains any workflows to be run. It is important to note, that the `workflow.xml` files must be on HDFS in order for Oozie to pick them up (this is because Oozie typically runs on a separate machine as any client process).

`gobblin-oozie-example-workflow.xml` contains an example Oozie workflow. This example simply launches a Java process that invokes the main method of the [`CliLocalJobLauncher`](https://github.com/linkedin/gobblin/blob/master/gobblin-runtime/src/main/java/gobblin/runtime/local/CliLocalJobLauncher.java). The main method of this class expects two file paths to be passed to it (once again these files need to be on HDFS). The `jobconfig` arg should point to a file on HDFS containing all job configuration parameters. An example `jobconfig` file can be found [here](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull). The `sysconfig` arg should point to a file on HDFS containing all system configuration parameters. An example `sysconfig` file for Oozie can be found [here](https://github.com/linkedin/gobblin/blob/master/gobblin-oozie/src/main/resources/local/gobblin-oozie-example-system.properties).

<!---Ying Do you think we can add some descriptions about launching through MR mode? The simplest way is to use the <shell> tag and invoke `gobblin-mapreduce.sh`. I've tested it before.-->

#### Uploading Files to HDFS

Oozie only reads a job properties file from the local file system (e.g. `gobblin-oozie-example-workflow.properties`), it expects all other configuration and dependent files to be uploaded to HDFS. Specifically, it looks for these files under the directory specified by `oozie.wf.application.path` Make sure this is the case before trying to launch an Oozie job.

##### Adding Gobblin `jar` Dependencies

Gobblin has a number of `jar` dependencies that need to be used when launching a Gobblin job. These dependencies can be taken from the `gobblin-dist.tar.gz` file that is created after building Gobblin. The tarball should contain a `lib` folder will the necessary dependencies. This folder should be placed into a `lib` folder under the same same directory specified by `oozie.wf.application.path` in the `gobblin-oozie-example-workflow.properties` file.

#### Launching the Job

Assuming one has the [Oozie CLI](https://oozie.apache.org/docs/3.1.3-incubating/DG_CommandLineTool.html) installed, the job can be launched using the following command: `oozie job -config gobblin-oozie-example-workflow.properties -run`.

### Launching Gobblin in MapReduce Mode

Launching Gobblin in mapreduce Mode works quite similar to the local mode. In this mode, the oozie launcher action will spawn a second mapreduce process where gobblin will process its tasks in distributed mode across the cluster. Since each of the Mappers needs access to the gobblin libraries, we need to provide the jars via the `job.hdfs.jars` variable

#### Example Config Files

[`gobblin-oozie/src/main/resources/mapreduce`](https://github.com/linkedin/gobblin/tree/master/gobblin-oozie/src/main/resources/mapreduce) contains sample configuration files for launching Gobblin Oozie in Mapreduce mode. The main difference to launching Gobblin Oozie in Local mode are a view extra MapReduce related configuration variables in the sysconfig.properties file and launching CliMRJobLauncher instead CliLocalJobLauncher.

#### Further steps

Everything else should be working the same way as in Local mode (see above)

### Debugging Tips

Once the job has been launched, its status can be queried via the following command: `oozie job -info <oozie-job-id>` and the logs can be shown via the following command `oozie job -log <oozie-job-id>`.

In order to get see the standard output of Gobblin, one needs to check the logs the Map task running the Gobblin process. `oozie job -info <oozie-job-id>` should show the Hadoop `job_id` of the Hadoop Job launched to run the Gobblin process. Using this id one should be able to find the logs of the Map tasks through the UI or other command line tools (e.g. `yarn logs`).
