Configuration properties are key/value pairs that are set in text files. They include system properties that control how Gobblin will pull data, and control what source Gobblin will pull the data from. Configuration files end in some user-specified suffix (by default text files ending in `.pull` or `.job` are recognized as configs files, although this is configurable). Each file represents some unit of work that needs to be done in Gobblin. For example, there will typically be a separate configuration file for each table that needs to be pulled from a database.  
  
The first section of this document contains all the required properties needed to run a basic Gobblin job. The rest of the document is dedicated to other properties that can be used to configure Gobbin jobs. The description of each configuration parameter will often refer to core Gobblin concepts and terms. If any of these terms are confusing, check out the [Gobblin Archiecture](../Gobblin-Architecture) page for a more detailed explanation of how Gobblin works. The GitHub repo also contains sample config files for specific sources. For example, there are sample config files to connect to MySQL databases and [SFTP servers](https://github.com/linkedin/gobblin/tree/master/source/src/main/resources).  

Gobblin also allows you to specify a global configuration file that contains common properties that are shared across all jobs. The [Job Launcher Properties](#Job-Launcher-Properties) section has more information on how to specify a global properties file.  

# Table of Contents
* [Properties File Format](#Properties-File-Format)
* [Creating a Basic Properties File](#Creating-a-Basic-Properties-File)   
* [Job Launcher Properties](#Job-Launcher-Properties)  
  * [Common Job Launcher Properties](#Common-Launcher-Properties)  
  * [SchedulerDaemon Properties](#SchedulerDaemon-Properties)  
  * [CliMRJobLauncher Properties](#CliMRJobLauncher-Properties)  
  * [AzkabanJobLauncher Properties](#AzkabanJobLauncher-Properties)  
* [Job Type Properties](#Job-Type-Properties)  
  * [Common Job Type Properties](#Common-Type-Properties)  
  * [LocalJobLauncher Properties](#LocalJobLauncher-Properties)  
  * [MRJobLauncher Properties](#MRJobLauncher-Properties)  
* [Task Execution Properties](#Task-Execution-Properties)  
* [State Store Properties](#State-Store-Properties)  
* [Metrics Properties](#Metrics-Properties)  
* [Email Alert Properties](#Email-Alert-Properties)  
* [Source Properties](#Source-Properties)  
  * [Common Source Properties](#Common-Source-Properties)  
  * [QueryBasedExtractor Properties](#QueryBasedExtractor-Properties) 
    * [JdbcExtractor Properties](#JdbcExtractor-Properties)  
  * [FileBasedExtractor Properties](#FileBasedExtractor-Properties)  
    * [SftpExtractor Properties](#SftpExtractor-Properties)  
* [Converter Properties](#Converter-Properties)
  * [CsvToJsonConverter Properties](#CsvToJsonConverter-Properties)    
  * [JsonIntermediateToAvroConverter Properties](#JsonIntermediateToAvroConverter-Properties)
  * [JsonStringToJsonIntermediateConverter Properties](#JsonStringToJsonIntermediateConverter-Properties)
  * [AvroFilterConverter Properties](#AvroFilterConverter-Properties)  
  * [AvroFieldRetrieverConverter Properties](#AvroFieldRetrieverConverter-Properties)  
  * [AvroFieldsPickConverter Properties](#AvroFieldsPickConverter-Properties)  
  * [AvroToJdbcEntryConverter Properties](#AvroToJdbcEntryConverter-Properties)  
* [Quality Checker Properties](#Quality-Checker-Properties)  
* [Writer Properties](#Writer-Properties)  
* [Data Publisher Properties](#Data-Publisher-Properties)  
* [Generic Properties](#Generic-Properties)  
* [FileBasedJobLock Properties](#FileBasedJobLock-Properties)
* [ZookeeperBasedJobLock Properties](#ZookeeperBasedJobLock-Properties)
* [JDBC Writer Properties](#JdbcWriter-Properties)

# Properties File Format <a name="Properties-File-Format"></a>

Configuration properties files follow the [Java Properties text file format](http://docs.oracle.com/javase/7/docs/api/java/util/Properties.html#load(java.io.Reader)). Further, file includes and variable expansion/interpolation as defined in [Apache Commons Configuration](http://commons.apache.org/proper/commons-configuration/userguide_v1.10/user_guide.html) are also supported.

Example:

* common.properties

```
    writer.staging.dir=/path/to/staging/dir/
    writer.output.dir=/path/to/output/dir/
```
* my-job.properties

```    
    include=common.properties
    
    job.name=MyFirstJob
```

# Creating a Basic Properties File <a name="Creating-a-Basic-Properties-File"></a>
In order to create a basic configuration property there is a small set of required properties that need to be set. The following properties are required to run any Gobblin job:

* `job.name` - Name of the job  
* `source.class` - Fully qualified path to the Source class responsible for connecting to the data source  
* `writer.staging.dir` - The directory each task will write staging data to  
* `writer.output.dir` - The directory each task will commit data to  
* `data.publisher.final.dir` - The final directory where all the data will be published
* `state.store.dir` - The directory where state-store files will be written  

For more information on each property, check out the comprehensive list below.  

If only these properties are set, then by default, Gobblin will run in Local mode, as opposed to running on Hadoop M/R. This means Gobblin will write Avro data to the local filesystem. In order to write to HDFS, set the `writer.fs.uri` property to the URI of the HDFS NameNode that data should be written to. Since the default version of Gobblin writes data in Avro format, the writer expects Avro records to be passed to it. Thus, any data pulled from an external source must be converted to Avro before it can be written out to the filesystem.  

The `source.class` property is one of the most important properties in Gobblin. It specifies what Source class to use. The Source class is responsible for determining what work needs to be done during each run of the job, and specifies what Extractor to use in order to read over each sub-unit of data. Examples of Source classes are [WikipediaSource](https://github.com/linkedin/gobblin/blob/master/example/src/main/java/com/linkedin/uif/example/wikipedia/WikipediaSource.java) and [SimpleJsonSource](https://github.com/linkedin/gobblin/blob/master/example/src/main/java/com/linkedin/uif/example/simplejson/SimpleJsonSource.java), which can be found in the GitHub repository. For more information on Sources and Extractors, check out the [Architecture](Gobblin-Architecture) page.  

Typically, Gobblin jobs will be launched using the launch scripts in the `bin` folder. These scripts allow jobs to be launched on the local machine (e.g. SchedulerDaemon) or on Hadoop (e.g. CliMRJobLauncher). Check out the Job Launcher section below, to see the configuration difference between each launch mode. The [Deployment](Gobblin Deployment) page also has more information on the different ways a job can be launched.  

# Job Launcher Properties <a name="Job-Launcher-Properties"></a>
Gobblin jobs can be launched and scheduled in a variety of ways. They can be scheduled via a Quartz scheduler or through [Azkaban](https://github.com/azkaban/azkaban). Jobs can also be run without a scheduler via the Command Line. For more information on launching Gobblin jobs, check out the [Deployment](Gobblin-Deployment) page.
## Common Job Launcher Properties <a name="Common-Launcher-Properties"></a>
These properties are common to both the Job Launcher and the Command Line.
#### job.name 
###### Description
The name of the job to run. This name must be unique within a single Gobblin instance.
###### Default Value
None
###### Required
Yes
#### job.group 
###### Description
A way to group logically similar jobs together.
###### Default Value
None
###### Required
No
#### job.description 
###### Description
A description of what the jobs does.
###### Default Value
None
###### Required
No
#### job.lock.enabled
###### Description
If set to true job locks are enabled, if set to false they are disabled
###### Default Value
True
###### Required
No
#### job.lock.type
##### Description
The fully qualified name of the JobLock class to run. The JobLock is responsible for ensuring that only a single instance of a job runs at a time.
###### Default Value
`gobblin.runtime.locks.FileBasedJobLock`
###### Allowed Values
* [gobblin.runtime.locks.FileBasedJobLock](#FileBasedJobLock-Properties)
* [gobblin.runtime.locks.ZookeeperBasedJobLock](#ZookeeperBasedJobLock-Properties)

###### Required
No
#### job.runonce 
###### Description
A boolean specifying whether the job will be only once, or multiple times. If set to true the job will only be run once even if a job.schedule is specified. If set to false and a job.schedule is specified then it will run according to the schedule. If set false and a job.schedule is not specified, it will run only once.
###### Default Value
False 
###### Required
No
#### job.disabled 
###### Description
Whether the job is disabled or not. If set to true, then Gobblin will not run this job.
###### Default Value
False
###### Required
No
## SchedulerDaemon Properties <a name="SchedulerDaemon-Properties"></a>
This class is used to schedule Gobblin jobs on Quartz. The job can be launched via the command line, and takes in the location of a global configuration file as a parameter. This configuration file should have the property `jobconf.dir` in order to specify the location of all the `.job` or `.pull` files. Another core difference, is that the global configuration file for the SchedulerDaemon must specify the following properties:

* `writer.staging.dir`  
* `writer.output.dir`  
* `data.publisher.final.dir`  
* `state.store.dir`  

They should not be set in individual job files, as they are system-level parameters.
For more information on how to set the configuration parameters for jobs launched through the SchedulerDaemon, check out the [Deployment](Gobblin-Deployment) page.
#### job.schedule 
###### Description
Cron-Based job schedule. This schedule only applies to jobs that run using Quartz.
###### Default Value
None
###### Required
No
#### jobconf.dir
###### Description
When running in local mode, Gobblin will check this directory for any configuration files. Each configuration file should correspond to a separate Gobblin job, and each one should in a suffix specified by the jobconf.extensions parameter.
###### Default Value
None
###### Required
No
#### jobconf.extensions
###### Description
Comma-separated list of supported job configuration file extensions. When running in local mode, Gobblin will only pick up job files ending in these suffixes.
###### Default Value
pull,job
###### Required
No
#### jobconf.monitor.interval
###### Description
Controls how often Gobblin checks the jobconf.dir for new configuration files, or for configuration file updates. The parameter is measured in milliseconds.
###### Default Value
300000
###### Required
No
## CliMRJobLauncher Properties <a name="CliMRJobLauncher-Properties"></a>
There are no configuration parameters specific to CliMRJobLauncher. This class is used to launch Gobblin jobs on Hadoop from the command line, the jobs are not scheduled. Common properties are set using the `--sysconfig` option when launching jobs via the command line. For more information on how to set the configuration parameters for jobs launched through the command line, check out the [Deployment](Gobblin-Deployment) page.  
## AzkabanJobLauncher Properties <a name="AzkabanJobLauncher-Properties"></a>
There are no configuration parameters specific to AzkabanJobLauncher. This class is used to schedule Gobblin jobs on Azkaban. Common properties can be set through Azkaban by creating a `.properties` file, check out the [Azkaban Documentation](http://azkaban.github.io/) for more information. For more information on how to set the configuration parameters for jobs scheduled through the Azkaban, check out the [Deployment](Gobblin-Deployment) page.
# Job Type Properties <a name="Job-Type-Properties"></a>
## Common Job Type Properties <a name="Common-Job-Type-Properties"></a>
#### launcher.type 
###### Description
Job launcher type; one of LOCAL, MAPREDUCE, YARN. LOCAL mode runs on a single machine (LocalJobLauncher), MAPREDUCE runs on a Hadoop cluster (MRJobLauncher), and YARN runs on a YARN cluster (not implemented yet).
###### Default Value
LOCAL 
###### Required
No
## LocalJobLauncher Properties <a name="LocalJobLauncher-Properties"></a>
There are no configuration parameters specific to LocalJobLauncher. The LocalJobLauncher will launch a Hadoop job on a single machine. If launcher.type is set to LOCAL then this class will be used to launch the job.
Properties required by the MRJobLauncher class.
#### framework.jars 
###### Description
Comma-separated list of jars the Gobblin framework depends on. These jars will be added to the classpath of the job, and to the classpath of any containers the job launches.
###### Default Value
None
###### Required
No
#### job.jars 
###### Description
Comma-separated list of jar files the job depends on. These jars will be added to the classpath of the job, and to the classpath of any containers the job launches.
###### Default Value
None
###### Required
No
#### job.hdfs.jars 
###### Description
Comma-separated list of jar files the job depends on located in HDFS. These jars will be added to the classpath of the job, and to the classpath of any containers the job launches.
###### Default Value
None
###### Required
No
#### job.local.files 
###### Description
Comma-separated list of local files the job depends on. These files will be available to any map tasks that get launched via the DistributedCache.
###### Default Value
None
###### Required
No
#### job.hdfs.files 
###### Description
Comma-separated list of files on HDFS the job depends on. These files will be available to any map tasks that get launched via the DistributedCache.
###### Default Value
None
###### Required
No
## MRJobLauncher Properties <a name="MRJobLauncher-Properties"></a>
#### mr.job.root.dir 
###### Description
Working directory for a Gobblin Hadoop MR job. Gobblin uses this to write intermediate data, such as the workunit state files that are used by each map task. This has to be a path on HDFS.
###### Default Value
None
###### Required
Yes
#### mr.job.max.mappers 
###### Description
Maximum number of mappers to use in a Gobblin Hadoop MR job. If no explicit limit is set then a map task for each workunit will be launched. If the value of this properties is less than the number of workunits created, then each map task will run multiple tasks.
###### Default Value
None
###### Required
No
#### mr.include.task.counters 
###### Description
Whether to include task-level counters in the set of counters reported as Hadoop counters. Hadoop imposes a system-level limit (default to 120) on the number of counters, so a Gobblin MR job may easily go beyond that limit if the job has a large number of tasks and each task has a few counters. This property gives users an option to not include task-level counters to avoid going over that limit.
###### Default Value
False
###### Required
No
# Retry Properties <a name="Retry-Properties"></a>
Properties that control how tasks and jobs get retried on failure.
#### workunit.retry.enabled 
###### Description
Whether retries of failed work units across job runs are enabled or not.
###### Default Value
True 
###### Required
No
#### workunit.retry.policy 
###### Description
Work unit retry policy, can be one of {always, never, onfull, onpartial}.
###### Default Value
always 
###### Required
No
#### task.maxretries 
###### Description
Maximum number of task retries. A task will be re-tried this many times before it is considered a failure.
###### Default Value
5 
###### Required
No
#### task.retry.intervalinsec 
###### Description
Interval in seconds between task retries. The interval increases linearly with each retry. For example, if the first interval is 300 seconds, then the second one is 600 seconds, etc.
###### Default Value
300 
###### Required
No
#### job.max.failures 
###### Description
Maximum number of failures before an alert email is triggered.
###### Default Value
1 
# Task Execution Properties <a name="Task-Execution-Properties"></a>
These properties control how tasks get executed for a job. Gobblin uses thread pools in order to executes the tasks for a specific job. In local mode there is a single thread pool per job that executes all the tasks for a job. In MR mode there is a thread pool for each map task (or container), and all Gobblin tasks assigned to that mapper are executed in that thread pool.
#### taskexecutor.threadpool.size 
###### Description
Size of the thread pool used by task executor for task execution. Each task executor will spawn this many threads to execute any Tasks that is has been allocated.
###### Default Value
10 
###### Required
No
#### tasktracker.threadpool.coresize 
###### Description
Core size of the thread pool used by task tracker for task state tracking and reporting.
###### Default Value
10 
###### Required
No
#### tasktracker.threadpool.maxsize 
###### Description
Maximum size of the thread pool used by task tracker for task state tracking and reporting.
###### Default Value
10 
###### Required
No
#### taskretry.threadpool.coresize 
###### Description
Core size of the thread pool used by the task executor for task retries.
###### Default Value
2 
###### Required
No
#### taskretry.threadpool.maxsize 
###### Description
Maximum size of the thread pool used by the task executor for task retries.
###### Default Value
2 
###### Required
No
#### task.status.reportintervalinms 
###### Description
Task status reporting interval in milliseconds.
###### Default Value
30000 
###### Required
No
# State Store Properties <a name="State-Store-Properties"></a>
#### state.store.dir
###### Description
Root directory where job and task state files are stored. The state-store is used by Gobblin to track state between different executions of a job. All state-store files will be written to this directory.
###### Default Value
None
###### Required
Yes
#### state.store.fs.uri
###### Description
File system URI for file-system-based state stores.
###### Default Value
file:///
###### Required
No
# Metrics Properties <a name="Metrics-Properties"></a>
#### metrics.enabled
###### Description
Whether metrics collecting and reporting are enabled or not.
###### Default Value
True
###### Required
No
#### metrics.report.interval
###### Description
Metrics reporting interval in milliseconds.
###### Default Value
60000
###### Required
No
#### metrics.log.dir
###### Description
The directory where metric files will be written to.
###### Default Value
None
###### Required
No
#### metrics.reporting.file.enabled
###### Description
A boolean indicating whether or not metrics should be reported to a file.
###### Default Value
True
###### Required
No
#### metrics.reporting.jmx.enabled
###### Description
A boolean indicating whether or not metrics should be exposed via JMX.
###### Default Value
False
###### Required
No
# Email Alert Properties <a name="Email-Alert-Properties"></a>
#### email.alert.enabled 
###### Description
Whether alert emails are enabled or not. Email alerts are only sent out when jobs fail consecutively job.max.failures number of times.
###### Default Value
False 
###### Required
No
#### email.notification.enabled 
###### Description
Whether job completion notification emails are enabled or not. Notification emails are sent whenever the job completes, regardless of whether it failed or not.
###### Default Value
False 
###### Required
No
#### email.host 
###### Description
Host name of the email server.
###### Default Value
None
###### Required
Yes, if email notifications or alerts are enabled.
#### email.smtp.port 
###### Description
SMTP port number.
###### Default Value
None
###### Required
Yes, if email notifications or alerts are enabled.
#### email.user 
###### Description
User name of the sender email account.
###### Default Value
None
###### Required
No
#### email.password 
###### Description
User password of the sender email account.
###### Default Value
None
###### Required
No
#### email.from 
###### Description
Sender email address.
###### Default Value
None
###### Required
Yes, if email notifications or alerts are enabled.
#### email.tos 
###### Description
Comma-separated list of recipient email addresses.
###### Default Value
None
###### Required
Yes, if email notifications or alerts are enabled.
# Source Properties <a name="Source-Properties"></a>
## Common Source Properties <a name="Common-Source-Properties"></a>
These properties are common properties that are used among different Source implementations. Depending on what source class is being used, these parameters may or may not be necessary. These parameters are not tied to a specific source, and thus can be used in new source classes.
#### source.class 
###### Description
Fully qualified name of the Source class. For example, com.linkedin.gobblin.example.wikipedia
###### Default Value
None
###### Required
Yes
#### source.entity 
###### Description
Name of the source entity that needs to be pulled from the source. The parameter represents a logical grouping of data that needs to be pulled from the source. Often this logical grouping comes in the form a database table, a source topic, etc. In many situations, such as when using the QueryBasedExtractor, it will be the name of the table that needs to pulled from the source.
###### Default Value
None
###### Required
Required for QueryBasedExtractors, FileBasedExtractors.
#### source.timezone 
###### Description
Timezone of the data being pulled in by the extractor. Examples include "PST" or "UTC".
###### Default Value
None
###### Required
Required for QueryBasedExtractors
#### source.max.number.of.partitions 
###### Description
Maximum number of partitions to split this current run across. Only used by the QueryBasedSource and FileBasedSource.
###### Default Value
20 
###### Required
No
#### source.skip.first.record 
###### Description
True if you want to skip the first record of each data partition. Only used by the FileBasedExtractor.
###### Default Value
False 
###### Required
No
#### extract.namespace 
###### Description
Namespace for the extract data. The namespace will be included in the default file name of the outputted data.
###### Default Value
None
###### Required
No
#### source.conn.use.proxy.url 
###### Description
The URL of the proxy to connect to when connecting to the source. This parameter is only used for SFTP and REST sources.
###### Default Value
None
###### Required
No
#### source.conn.use.proxy.port 
###### Description
The port of the proxy to connect to when connecting to the source. This parameter is only used for SFTP and REST sources.
###### Default Value
None
###### Required
No
#### source.conn.username 
###### Description
The username to authenticate with the source. This is parameter is only used for SFTP and JDBC sources.
###### Default Value
None
###### Required
No
#### source.conn.password 
###### Description
The password to use when authenticating with the source. This is parameter is only used for JDBC sources.
###### Default Value
None
###### Required
No
#### source.conn.host 
###### Description
The name of the host to connect to.
###### Default Value
None
###### Required
Required for SftpExtractor, MySQLExtractor, OracleExtractor, SQLServerExtractor and TeradataExtractor.
#### source.conn.rest.url 
###### Description
URL to connect to for REST requests. This parameter is only used for the Salesforce source.
###### Default Value
None
###### Required
No
#### source.conn.version 
###### Description
Version number of communication protocol. This parameter is only used for the Salesforce source.
###### Default Value
None
###### Required
No
#### source.conn.timeout 
###### Description
The timeout set for connecting to the source in milliseconds.
###### Default Value
500000
###### Required
No
#### source.conn.port
###### Description
The value of the port to connect to.
###### Default Value
None
###### Required
Required for SftpExtractor, MySQLExtractor, OracleExtractor, SQLServerExtractor and TeradataExtractor.
#### source.conn.sid
###### Description
The Oracle System ID (SID) that identifies the database to connect to.
###### Default Value
None
###### Required
Required for OracleExtractor.
#### extract.table.name 
###### Description
Table name in Hadoop which is different table name in source.
###### Default Value
Source table name 
###### Required
No
#### extract.is.full 
###### Description
True if this pull should treat the data as a full dump of table from the source, false otherwise
###### Default Value
False 
###### Required
No
#### extract.delta.fields 
###### Description
List of columns that will be used as the delta field for the data.
###### Default Value
None
###### Required
No
#### extract.primary.key.fields 
###### Description
List of columns that will be used as the primary key for the data.
###### Default Value
None
###### Required
No
#### extract.pull.limit 
###### Description
This limits the number of records read by Gobblin. In Gobblin's extractor the readRecord() method is expected to return records until there are no more to pull, in which case it runs null. This parameter limits the number of times readRecord() is executed. This parameter is useful for pulling a limited sample of the source data for testing purposes.
###### Default Value
Unbounded
###### Required
No
#### extract.full.run.time 
###### Description

###### Default Value

###### Required

## QueryBasedExtractor Properties <a name="QueryBasedExtractor-Properties"></a>
The following table lists the query based extractor configuration properties.
#### source.querybased.watermark.type 
###### Description
The format of the watermark that is used when extracting data from the source. Possible types are timestamp, date, hour, simple.
###### Default Value
timestamp 
###### Required
Yes
#### source.querybased.start.value 
###### Description
Value for the watermark to start pulling data from, also the default watermark if the previous watermark cannot be found in the old task states.
###### Default Value
None
###### Required
Yes
#### source.querybased.partition.interval 
###### Description
Number of hours to pull in each partition.
###### Default Value
1 
###### Required
No
#### source.querybased.hour.column 
###### Description
Delta column with hour for hourly extracts (Ex: hour_sk)
###### Default Value
None
###### Required
No
#### source.querybased.skip.high.watermark.calc 
###### Description
If it is true, skips high watermark calculation in the source and it will use partition higher range as high watermark instead of getting it from source.
###### Default Value
False 
###### Required
No
#### source.querybased.query 
###### Description
The query that the extractor should execute to pull data.
###### Default Value
None
###### Required
No
#### source.querybased.hourly.extract 
###### Description
True if hourly extract is required.
###### Default Value
False 
###### Required
No
#### source.querybased.extract.type 
###### Description
"snapshot" for the incremental dimension pulls. "append_daily", "append_hourly" and "append_batch" for the append data append_batch for the data with sequence numbers as watermarks
###### Default Value
None
###### Required
No
#### source.querybased.end.value 
###### Description
The high watermark which this entire job should pull up to. If this is not specified, pull entire data from the table
###### Default Value
None
###### Required
No
#### source.querybased.append.max.watermark.limit 
###### Description
max limit of the high watermark for the append data.  CURRENT_DATE - X CURRENT_HOUR - X where X>=1
###### Default Value
CURRENT_DATE for daily extract CURRENT_HOUR for hourly extract 
###### Required
No
#### source.querybased.is.watermark.override 
###### Description
True if this pull should override previous watermark with start.value and end.value. False otherwise.
###### Default Value
False 
###### Required
No
#### source.querybased.low.watermark.backup.secs 
###### Description
Number of seconds that needs to be backup from the previous high watermark. This is to cover late data.  Ex: Set to 3600 to cover 1 hour late data.
###### Default Value
0 
###### Required
No
#### source.querybased.schema 
###### Description
Database name
###### Default Value
None
###### Required
No
#### source.querybased.is.specific.api.active 
###### Description
True if this pull needs to use source specific apis instead of standard protocols.  Ex: Use salesforce bulk api instead of rest api
###### Default Value
False 
###### Required
No
#### source.querybased.skip.count.calc
###### Description
A boolean, if true then the QueryBasedExtractor will skip the source count calculation.
###### Default Value
False 
###### Required
No
#### source.querybased.fetch.size
###### Description
This parameter is currently only used in JDBCExtractor. The JDBCExtractor will process this many number of records from the JDBC ResultSet at a time. It will then take these records and return them to the rest of the Gobblin flow so that they can get processed by the rest of the Gobblin components. 
###### Default Value
1000
###### Required
No
#### source.querybased.is.metadata.column.check.enabled
###### Description
When a query is specified in the configuration file, it is possible a user accidentally adds in a column name that does not exist on the source side. By default, this parameter is set to false, which means that if a column is specified in the query and it does not exist in the source data set, Gobblin will just skip over that column. If it is set to true, Gobblin will actually take the config specified column and check to see if it exists in the source data set. If it doesn't exist then the job will fail.
###### Default Value
False
###### Required
No
#### source.querybased.is.compression.enabled
###### Description
A boolean specifying whether or not compression should be enabled when pulling data from the source. This parameter is only used for MySQL sources. If set to true, the MySQL will send compressed data back to the source.
###### Default Value
False
###### Required
No
#### source.querybased.jdbc.resultset.fetch.size
###### Description
The number of rows to pull through JDBC at a time. This is useful when the JDBC ResultSet is too big to fit into memory, so only "x" number of records will be fetched at a time.
###### Default Value
1000
###### Required
No
### JdbcExtractor Properties <a name="JdbcExtractor-Properties"></a>
The following table lists the jdbc based extractor configuration properties.
#### source.conn.driver
###### Description
The fully qualified path of the JDBC driver used to connect to the external source.
###### Default Value
None
###### Required
Yes
#### source.column.name.case
###### Description
A enum specifying whether or not to convert the column names to a specific case before performing a query. Possible values are TOUPPER or TOLOWER.
###### Default Value
NOCHANGE 
###### Required
No
## FileBasedExtractor Properties <a name="FileBasedExtractor-Properties"></a>
The following table lists the file based extractor configuration properties.
#### source.filebased.data.directory 
###### Description
The data directory from which to pull data from.
###### Default Value
None
###### Required
Yes
#### source.filebased.files.to.pull 
###### Description
A list of files to pull - this should be set in the Source class and the extractor will pull the specified files.
###### Default Value
None
###### Required
Yes  
#### filebased.report.status.on.count
###### Description
The FileBasedExtractor will report it's status every time it processes the number of records specified by this parameter. The way it reports status is by logging out how many records it has seen.
###### Default Value
10000
###### Required
No  
#### source.filebased.fs.uri
###### Description
The URI of the filesystem to connect to.
###### Default Value
None
###### Required
Required for HadoopExtractor.
#### source.filebased.preserve.file.name
###### Description
A boolean, if true then the original file names will be preserved when they are are written to the source.
###### Default Value
False
###### Required
No
#### source.schema
###### Description
The schema of the data that will be pulled by the source.
###### Default Value
None
###### Required
Yes
### SftpExtractor Properties <a name="SftpExtractor-Properties"></a>
#### source.conn.private.key 
###### Description
File location of the private key used for key based authentication. This parameter is only used for the SFTP source.
###### Default Value
None
###### Required
Yes
#### source.conn.known.hosts 
###### Description
File location of the known hosts file used for key based authentication.
###### Default Value
None
###### Required
Yes
# Converter Properties <a name="Converter-Properties"></a>
Properties for Gobblin converters.
#### converter.classes 
###### Description
Comma-separated list of fully qualified names of the Converter classes. The order is important as the converters will be applied in this order.
###### Default Value
None
###### Required
No
## CsvToJsonConverter Properties <a name="CsvToJsonConverter-Properties"></a>
This converter takes in text data separated by a delimiter (converter.csv.to.json.delimiter), and splits the data into a JSON format recognized by JsonIntermediateToAvroConverter.
#### converter.csv.to.json.delimiter
###### Description
The regex delimiter between CSV based files, only necessary when using the CsvToJsonConverter - e.g. ",", "/t" or some other regex
###### Default Value
None
###### Required
Yes
## JsonIntermediateToAvroConverter Properties <a name="JsonIntermediateToAvroConverter-Properties"></a>
This converter takes in JSON data in a specific schema, and converts it to Avro data.
#### converter.avro.date.format
###### Description
Source format of the date columns for Avro-related converters.
###### Default Value
None
###### Required
No
#### converter.avro.timestamp.format
###### Description
Source format of the timestamp columns for Avro-related converters.
###### Default Value
None
###### Required
No
#### converter.avro.time.format
###### Description
Source format of the time columns for Avro-related converters.
###### Default Value
None
###### Required
No
#### converter.avro.binary.charset
###### Description
Source format of the time columns for Avro-related converters.
###### Default Value
UTF-8
###### Required
No
#### converter.is.epoch.time.in.seconds
###### Description
A boolean specifying whether or not a epoch time field in the JSON object is in seconds or not.
###### Default Value
None
###### Required
Yes
#### converter.avro.max.conversion.failures
###### Description
This converter is will fail for this many number of records before throwing an exception.
###### Default Value
0
###### Required
No
#### converter.avro.nullify.fields.enabled
###### Description
Generate new avro schema by nullifying fields that previously existed but not in the current schema.
###### Default Value
false
###### Required
No
#### converter.avro.nullify.fields.original.schema.path
###### Description
Path of the original avro schema which will be used for merging and nullify fields.
###### Default Value
None
###### Required
No
## JsonStringToJsonIntermediateConverter Properties <a name="JsonStringToJsonIntermediateConverter-Properties"></a>
#### gobblin.converter.jsonStringToJsonIntermediate.unpackComplexSchemas
###### Description
Parse nested JSON record using source.schema.
###### Default Value
True
###### Required
No
## AvroFilterConverter Properties <a name="AvroFilterConverter-Properties"></a>
This converter takes in an Avro record, and filters out records by performing an equality operation on the value of the field specified by converter.filter.field and the value specified in converter.filter.value. It returns the record unmodified if the equality operation evaluates to true, false otherwise.
#### converter.filter.field
###### Description
The name of the field in the Avro record, for which the converter will filter records on.
###### Default Value
None
###### Required
Yes
#### converter.filter.value
###### Description
The value that will be used in the equality operation to filter out records.
###### Default Value
None
###### Required
Yes
## AvroFieldRetrieverConverter Properties <a name="AvroFieldRetrieverConverter-Properties"></a>
This converter takes a specific field from an Avro record and returns its value.
#### converter.avro.extractor.field.path
###### Description
The field in the Avro record to retrieve. If it is a nested field, then each level must be separated by a period.
###### Default Value
None
###### Required
Yes
## AvroFieldsPickConverter Properties <a name="AvroFieldsPickConverter-Properties"></a>
Unlike AvroFieldRetriever, this converter takes multiple fields from Avro schema and convert schema and generic record.
#### converter.avro.fields
###### Description
Comma-separted list of the fields in the Avro record. If it is a nested field, then each level must be separated by a period.
###### Default Value
None
###### Required
Yes
## AvroToJdbcEntryConverter Properties <a name="AvroToJdbcEntryConverter-Properties"></a>
Converts Avro schema and generic record into Jdbc entry schema and data.
#### converter.avro.jdbc.entry_fields_pairs
###### Description
Converts Avro field name(s) to fit for JDBC underlying data base. Input format is key value pairs of JSON array where key is avro field name and value is corresponding JDBC column name.
###### Default Value
None
###### Required
No
# Fork Properties <a name="Fork-Properties"></a>
Properties for Gobblin's fork operator.
#### fork.operator.class 
###### Description
Fully qualified name of the ForkOperator class.
###### Default Value
com.linkedin.uif.fork.IdentityForkOperator 
###### Required
No
#### fork.branches 
###### Description
Number of fork branches.
###### Default Value
1 
###### Required
No
#### fork.branch.name.${branch index} 
###### Description
Name of a fork branch with the given index, e.g., 0 and 1.
###### Default Value
fork_${branch index}, e.g., fork_0 and fork_1. 
###### Required
No
# Quality Checker Properties <a name="Quality-Checker-Properties"></a>
#### qualitychecker.task.policies 
###### Description
Comma-separted list of fully qualified names of the TaskLevelPolicy classes that will run at the end of each Task.
###### Default Value
None
###### Required
No
#### qualitychecker.task.policy.types 
###### Description
OPTIONAL implies the corresponding class in qualitychecker.task.policies is optional and if it fails the Task will still succeed, FAIL implies that if the corresponding class fails then the Task will fail too.
###### Default Value
OPTIONAL 
###### Required
No
#### qualitychecker.row.policies 
###### Description
Comma-separted list of fully qualified names of the RowLevelPolicy classes that will run on each record.
###### Default Value
None
###### Required
No
#### qualitychecker.row.policy.types 
###### Description
OPTIONAL implies the corresponding class in qualitychecker.row.policies is optional and if it fails the Task will still succeed, FAIL implies that if the corresponding class fails then the Task will fail too, ERR_FILE implies that if the record does not pass the test then the record will be written to an error file.
###### Default Value
OPTIONAL 
###### Required
No
#### qualitychecker.row.err.file 
###### Description
The quality checker will write the current record to the location specified by this parameter, if the current record fails to pass the quality checkers specified by qualitychecker.row.policies; this file will only be written to if the quality checker policy type is ERR_FILE.
###### Default Value
None
###### Required
No
# Writer Properties <a name="Writer-Properties"></a>
#### writer.destination.type 
###### Description
Writer destination type. Can be HDFS, KAFKA, MYSQL or TERADATA
###### Default Value
HDFS 
###### Required
No
#### writer.output.format 
###### Description
Writer output format; currently only Avro is supported.
###### Default Value
AVRO 
###### Required
No
#### writer.fs.uri 
###### Description
File system URI for writer output.
###### Default Value
file:/// 
###### Required
No
#### writer.staging.dir 
###### Description
Staging directory of writer output. All staging data that the writer produces will be placed in this directory, but all the data will be eventually moved to the writer.output.dir.
###### Default Value
None
###### Required
Yes
#### writer.output.dir 
###### Description
Output directory of writer output. All output data that the writer produces will be placed in this directory, but all the data will be eventually moved to the final directory by the publisher.
###### Default Value
None
###### Required
Yes
#### writer.builder.class 
###### Description
Fully qualified name of the writer builder class.
###### Default Value
com.linkedin.uif.writer.AvroDataWriterBuilder
###### Required
No
#### writer.file.path 
###### Description
The Path where the writer will write it's data. Data in this directory will be copied to it's final output directory by the DataPublisher.
###### Default Value
None
###### Required
Yes
#### writer.file.name 
###### Description
The name of the file the writer writes to.
###### Default Value
part 
###### Required
Yes

#### writer.partitioner.class
###### Description
Partitioner used for distributing records into multiple output files. `writer.builder.class` must be a subclass of `PartitionAwareDataWriterBuilder`, otherwise Gobblin will throw an error. 
###### Default Value
None (will not use partitioner)
###### Required
No

#### writer.buffer.size 
###### Description
Writer buffer size in bytes. This parameter is only applicable for the AvroHdfsDataWriter.
###### Default Value
4096 
###### Required
No
#### writer.deflate.level 
###### Description
Writer deflate level. Deflate is a type of compression for Avro data.
###### Default Value
9 
###### Required
No
#### writer.codec.type 
###### Description
This is used to specify the type of compression used when writing data out. Possible values are NOCOMPRESSION, DEFLATE, SNAPPY.
###### Default Value
DEFLATE 
###### Required
No
#### writer.eager.initialization
###### Description
This is used to control the writer creation. If the value is set to true, writer is created before records are read. This means an empty file will be created even if no records were read.
###### Default Value
False 
###### Required
No
#### writer.parquet.page.size
###### Description
The page size threshold
###### Default Value
1048576
###### Required
No
#### writer.parquet.dictionary.page.size
###### Description
The block size threshold.
###### Default Value
134217728
###### Required
No
#### writer.parquet.dictionary
###### Description
To turn dictionary encoding on.
###### Default Value
true
###### Required
No
#### writer.parquet.validate
###### Description
To turn on validation using the schema.
###### Default Value
false
###### Required
No
#### writer.parquet.version
###### Description
Version of parquet writer to use. Available versions are v1 and v2.
###### Default Value
v1
###### Required
No
# Data Publisher Properties <a name="Data-Publisher-Properties"></a>
#### data.publisher.type 
###### Description
The fully qualified name of the DataPublisher class to run. The DataPublisher is responsible for publishing task data once all Tasks have been completed.
###### Default Value
None
###### Required
Yes
#### data.publisher.final.dir 
###### Description
The final output directory where the data should be published.
###### Default Value
None
###### Required
Yes
#### data.publisher.replace.final.dir
###### Description
A boolean, if true and the the final output directory already exists, then the data will not be committed. If false and the final output directory already exists then it will be overwritten.
###### Default Value
None
###### Required
Yes
#### data.publisher.final.name
###### Description
The final name of the file that is produced by Gobblin. By default, Gobblin already assigns a unique name to each file it produces. If that default name needs to be overridden then this parameter can be used. Typically, this parameter should be set on a per workunit basis so that file names don't collide.
###### Default Value

###### Required
No
# Generic Properties <a name="Generic-Properties"></a>
These properties are used throughout multiple Gobblin components.
#### fs.uri
###### Description
Default file system URI for all file storage; over-writable by more specific configuration properties.
###### Default Value
file:///
###### Required
No
# FileBasedJobLock Properties <a name="FileBasedJobLock-Properties"></a>
#### job.lock.dir
###### Description
Directory where job locks are stored. Job locks are used by the scheduler to ensure two executions of a job do not run at the same time. If a job is scheduled to run, Gobblin will first check this directory to see if there is a lock file for the job. If there is one, it will not run the job, if there isn't one then it will run the job.
###### Default Value
None
###### Required
No
# ZookeeperBasedJobLock Properties <a name="ZookeeperBasedJobLock-Properties"></a>
#### zookeeper.connection.string
###### Description
The connection string to the ZooKeeper cluster used to manage the lock.
###### Default Value
localhost:2181
###### Required
No
#### zookeeper.session.timeout.seconds
###### Description
The zookeeper session timeout.
###### Default Value
180
###### Required
No
#### zookeeper.connection.timeout.seconds
###### Description
The zookeeper conection timeout.
###### Default Value
30
###### Required
No
#### zookeeper.retry.backoff.seconds
###### Description
The amount of time in seconds to wait between retries.  This will increase exponentially when retries occur.
###### Default Value
1
###### Required
No
#### zookeeper.retry.count.max
###### Description
The maximum number of times to retry.
###### Default Value
10
###### Required
No
#### zookeeper.locks.acquire.timeout.milliseconds
###### Description
The amount of time in milliseconds to wait while attempting to acquire the lock.
###### Default Value
5000
###### Required
No
#### zookeeper.locks.reaper.threshold.seconds
###### Description
The threshold in seconds that determines when a lock path can be deleted.
###### Default Value
300
###### Required
No

# JDBC Writer properties <a name="JdbcWriter-Properties"></a>
Writer(and publisher) that writes to JDBC database. Please configure below two properties to use JDBC writer & publisher.

*  writer.builder.class=org.apache.gobblin.writer.JdbcWriterBuilder
*  data.publisher.type=org.apache.gobblin.publisher.JdbcPublisher

#### jdbc.publisher.database_name
###### Description
Destination database name
###### Default Value
None
###### Required
Yes
#### jdbc.publisher.table_name
###### Description
Destination table name
###### Default Value
None
###### Required
Yes
#### jdbc.publisher.replace_table
###### Description
Gobblin will replace the data in destination table.
###### Default Value
false
###### Required
No
#### jdbc.publisher.username
###### Description
User name to connect to destination database
###### Default Value
None
###### Required
Yes
#### jdbc.publisher.password
###### Description
Password to connect to destination database. Also, accepts encrypted password.
###### Default Value
None
###### Required
Yes
#### jdbc.publisher.encrypt_key_loc
###### Description
Location of a key to decrypt an encrypted password
###### Default Value
None
###### Required
No
#### jdbc.publisher.url
###### Description
Connection URL
###### Default Value
None
###### Required
Yes
#### jdbc.publisher.driver
###### Description
JDBC driver class 
###### Default Value
None
###### Required
Yes
#### writer.staging.table
###### Description
User can pass staging table for Gobblin to use instead of Gobblin to create one. (e.g: For the user who does not have create table previlege can pass staging table for Gobblin to use).
###### Default Value
None
###### Required
No
#### writer.truncate.staging.table
###### Description
Truncate staging table if user passed their own staging table via "writer.staging.table".
###### Default Value
false
###### Required
No
#### writer.jdbc.batch_size
###### Description
Batch size for Insert operation
###### Default Value
30
###### Required
No
#### writer.jdbc.insert_max_param_size
###### Description
Maximum number of parameters for JDBC insert operation (for MySQL Writer).
###### Default Value
100,000 (MySQL limitation)
###### Required
No
