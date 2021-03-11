Table of Contents
---------------------------------------

[TOC]

# Gobblin

## General Questions <a name="General-Questions"></a>

##### What is Gobblin?

Gobblin is a universal ingestion framework. It's goal is to pull data from any source into an arbitrary data store. One major use case for Gobblin is pulling data into Hadoop. Gobblin can pull data from file systems, SQL stores, and data that is exposed by a REST API. See the Gobblin [Home](../index) page for more information.

##### What programming languages does Gobblin support?

Gobblin currently only supports Java 7 and up.

##### Does Gobblin require any external software to be installed?

The machine that Gobblin is built on must have Java installed, and the `$JAVA_HOME` environment variable must be set.

##### What Hadoop versions can Gobblin run on?

Gobblin can only be run on Hadoop 2.x. By default, Gobblin compiles against Hadoop 2.3.0.

##### How do I run and schedule a Gobblin job?

Check out the [Deployment](Gobblin Deployment) page for information on how to run and schedule Gobblin jobs. Check out the [Configuration](Configuration Properties Glossary) page for information on how to set proper configuration properties for a job.

##### How is Gobblin different from Sqoop?

Sqoop main focus bulk import and export of data from relational databases to HDFS, it lacks the ETL functionality of data cleansing, data transformation, and data quality checks that Gobblin provides. Gobblin is also capable of pulling from any data source (e.g. file systems, RDMS, REST APIs).

## Technical Questions <a name="Technical-Questions"></a>

##### When running on Hadoop, each map task quickly reaches 100 Percent completion, but then stalls for a long time. Why does this happen?

Gobblin currently uses Hadoop map tasks as a container for running Gobblin tasks. Each map task runs 1 or more Gobblin workunits, and the progress of each workunit is not hooked into the progress of each map task. Even though the Hadoop job reports 100% completion, Gobblin is still doing work. See the [Gobblin Deployment](Gobblin Deployment) page for more information.

##### Why does Gobblin on Hadoop stall for a long time between adding files to the DistrbutedCache, and launching the actual job?

Gobblin takes all WorkUnits created by the Source class and serializes each one into a file on Hadoop. These files are read by each map task, and are deserialized into Gobblin Tasks. These Tasks are then run by the map-task. The reason the job stalls is that Gobblin is writing all these files to HDFS, which can take a while especially if there are a lot of tasks to run. See the [Gobblin Deployment](Gobblin Deployment) page for more information.

##### How do I fix `UnsupportedFileSystemException: No AbstractFileSystem for scheme: null`?

This error typically occurs due to Hadoop version conflict issues. If Gobblin is compiled against a specific Hadoop version, but then deployed on a different Hadoop version or installation, this error may be thrown. For example, if you simply compile Gobblin using `./gradlew clean build`, but deploy Gobblin to a cluster with [CDH](https://www.cloudera.com/content/www/en-us/products/apache-hadoop/key-cdh-components.html) installed, you may hit this error.

It is important to realize that the the `gobblin-dist.tar.gz` file produced by `./gradlew clean build` will include all the Hadoop jar dependencies; and if one follows the [MR deployment guide](Gobblin-Deployment#Hadoop-MapReduce-Deployment), Gobblin will be launched with these dependencies on the classpath.

To fix this take the following steps:

* Delete all the Hadoop jars from the Gobblin `lib` folder
* Ensure that the environment variable `HADOOP_CLASSPATH` is set and points to a directory containing the Hadoop libraries for the cluster

##### How do I compile Gobblin against CDH?

[Cloudera Distributed Hadoop](https://www.cloudera.com/content/www/en-us/products/apache-hadoop/key-cdh-components.html) (often abbreviated as CDH) is a popular Hadoop distribution. Typically, when running Gobblin on a CDH cluster it is recommended that one also compile Gobblin against the same CDH version. Not doing so may cause unexpected runtime behavior. To compile against a specific CDH version simply use the `hadoopVersion` parameter. For example, to compile against version `2.5.0-cdh5.3.0` run `./gradlew clean build -PhadoopVersion=2.5.0-cdh5.3.0`.

##### Resolve Gobblin-on-MR Exception `IOException: Not all tasks running in mapper attempt_id completed successfully`

This exception usually just means that a Hadoop Map Task running Gobblin Tasks threw some exception. Unfortunately, the exception isn't truly indicative of the underlying problem, all it is really saying is that something went wrong in the Gobblin Task. Each Hadoop Map Task has its own log file and it is often easiest to look at the logs of the Map Task when debugging this problem. There are multiple ways to do this, but one of the easiest ways is to execute `yarn logs -applicationId <application ID> [OPTIONS]`

##### Gradle Build Fails With `Cannot invoke method getURLs on null object`

Add `-x test` to build the project without running the tests; this will make the exception go away. If one needs to run the tests then make sure [Java Cryptography Extension](https://en.wikipedia.org/wiki/Java_Cryptography_Extension) is installed.

This exception also occurs when $JAVE_HOME not properly set, especially trying to build in Intellij/Eclipse. Try to execute `export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.{your JDK version}.jdk/Contents/Home` in your IDE terminal.

# Gradle

## Technical Questions

#### How do I add a new external dependency?

Say I want to add [`oozie-core-4.2.0.jar`](http://mvnrepository.com/artifact/org.apache.oozie/oozie-core/4.2.0) as a dependency to the `gobblin-scheduler` subproject. I would first open the file `build.gradle` and add the following entry to the `ext.externalDependency` array: `"oozieCore": "org.apache.oozie:oozie-core:4.2.0"`.

Then in the `gobblin-scheduler/build.gradle` file I would add the following line to the dependency block: `compile externalDependency.oozieCore`.

#### How do I add a new Maven Repository to pull artifacts from?

Often times, one may have important artifacts stored in a local or private Maven repository. As of 01/21/2016 Gobblin only pulls artifacts from the following Maven Repositories: [Maven Central](http://repo1.maven.org/maven/), [Conjars](http://conjars.org/repo), and [Cloudera](https://repository.cloudera.com/artifactory/cloudera-repos/).

In order to add another Maven Repository modify the `defaultEnvironment.gradle` file and the new repository using the same pattern as the existing ones.
