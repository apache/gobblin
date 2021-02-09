Configuration properties are key/value pairs that are set in text files. They include system properties that control how Gobblin will pull data, and control what source Gobblin will pull the data from. Configuration files end in some user-specified suffix (by default text files ending in `.pull` or `.job` are recognized as configs files, although this is configurable). Each file represents some unit of work that needs to be done in Gobblin. For example, there will typically be a separate configuration file for each table that needs to be pulled from a database.  
  
The first section of this document contains all the required properties needed to run a basic Gobblin job. The rest of the document is dedicated to other properties that can be used to configure Gobbin jobs. The description of each configuration parameter will often refer to core Gobblin concepts and terms. If any of these terms are confusing, check out the [Gobblin Architecture](../Gobblin-Architecture) page for a more detailed explanation of how Gobblin works. The GitHub repo also contains sample config files for specific sources. For example, there are sample config files to connect to [MySQL databases](https://github.com/apache/gobblin/tree/master/gobblin-core/src/main/resources/mysql) and [SFTP servers](https://github.com/apache/gobblin/tree/master/gobblin-core/src/main/resources/sftp).  

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
    * [Common Job Type Properties](#Common-Job-Type-Properties)  
    * [LocalJobLauncher Properties](#LocalJobLauncher-Properties)  
    * [MRJobLauncher Properties](#MRJobLauncher-Properties)  
* [Retry Properties](#Retry-Properties)
* [Task Execution Properties](#Task-Execution-Properties)  
* [State Store Properties](#State-Store-Properties)  
* [Metrics Properties](#Metrics-Properties)  
* [Email Alert Properties](#Email-Alert-Properties)  
* [Source Properties](#Source-Properties)  
    * [Common Source Properties](#Common-Source-Properties)  
    * [Distcp CopySource Properties](#Distcp-CopySource-Properties)
        * [RecursiveCopyableDataset Properties](#RecursiveCopyableDataset-Properties)
        * [DistcpFileSplitter Properties](#DistcpFileSplitter-Properties)
        * [WorkUnitBinPacker Properties](#WorkUnitBinPacker-Properties)
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
* [Fork Properties](#Fork-Properties)
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

The `source.class` property is one of the most important properties in Gobblin. It specifies what Source class to use. The Source class is responsible for determining what work needs to be done during each run of the job, and specifies what Extractor to use in order to read over each sub-unit of data. Examples of Source classes are [WikipediaSource](https://github.com/apache/gobblin/blob/master/gobblin-example/src/main/java/org/apache/gobblin/example/wikipedia/WikipediaSource.java) and [SimpleJsonSource](https://github.com/apache/gobblin/blob/master/gobblin-example/src/main/java/org/apache/gobblin/example/simplejson/SimpleJsonSource.java), which can be found in the GitHub repository. For more information on Sources and Extractors, check out the [Architecture](../Gobblin-Architecture) page.  

Typically, Gobblin jobs will be launched using the launch scripts in the `bin` folder. These scripts allow jobs to be launched on the local machine (e.g. SchedulerDaemon) or on Hadoop (e.g. CliMRJobLauncher). Check out the Job Launcher section below, to see the configuration difference between each launch mode. The [Deployment](Gobblin-Deployment) page also has more information on the different ways a job can be launched.  

# Job Launcher Properties <a name="Job-Launcher-Properties"></a>
Gobblin jobs can be launched and scheduled in a variety of ways. They can be scheduled via a Quartz scheduler or through [Azkaban](https://github.com/azkaban/azkaban). Jobs can also be run without a scheduler via the Command Line. For more information on launching Gobblin jobs, check out the [Deployment](Gobblin-Deployment) page.

## Common Job Launcher Properties <a name="Common-Launcher-Properties"></a>
These properties are common to both the Job Launcher and the Command Line.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `job.name` | The name of the job to run. This name must be unique within a single Gobblin instance. | Yes | None |
| `job.group` | A way to group logically similar jobs together. | No | None |
| `job.description` | A description of what the jobs does. | No | None |
| `job.lock.enabled` | If set to true job locks are enabled, if set to false they are disabled | No | True |
| `job.lock.type` | The fully qualified name of the JobLock class to run. The JobLock is responsible for ensuring that only a single instance of a job runs at a time. <br><br> Allowed values: [gobblin.runtime.locks.FileBasedJobLock](#FileBasedJobLock-Properties), [gobblin.runtime.locks.ZookeeperBasedJobLock](#ZookeeperBasedJobLock-Properties) | No | `gobblin.runtime.locks.FileBasedJobLock` |
| `job.runonce` | A boolean specifying whether the job will be only once, or multiple times. If set to true the job will only be run once even if a job.schedule is specified. If set to false and a job.schedule is specified then it will run according to the schedule. If set false and a job.schedule is not specified, it will run only once. | No | False |
| `job.disabled` | Whether the job is disabled or not. If set to true, then Gobblin will not run this job. | No | False |

## SchedulerDaemon Properties <a name="SchedulerDaemon-Properties"></a>
This class is used to schedule Gobblin jobs on Quartz. The job can be launched via the command line, and takes in the location of a global configuration file as a parameter. This configuration file should have the property `jobconf.dir` in order to specify the location of all the `.job` or `.pull` files. Another core difference, is that the global configuration file for the SchedulerDaemon must specify the following properties:

* `writer.staging.dir`  
* `writer.output.dir`  
* `data.publisher.final.dir`  
* `state.store.dir`  

They should not be set in individual job files, as they are system-level parameters.
For more information on how to set the configuration parameters for jobs launched through the SchedulerDaemon, check out the [Deployment](Gobblin-Deployment) page.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `job.schedule` | Cron-Based job schedule. This schedule only applies to jobs that run using Quartz. | No | None |
| `jobconf.dir` | When running in local mode, Gobblin will check this directory for any configuration files. Each configuration file should correspond to a separate Gobblin job, and each one should in a suffix specified by the jobconf.extensions parameter. | No | None |
| `jobconf.extensions` | Comma-separated list of supported job configuration file extensions. When running in local mode, Gobblin will only pick up job files ending in these suffixes. | No | pull,job |
| `jobconf.monitor.interval` | Controls how often Gobblin checks the jobconf.dir for new configuration files, or for configuration file updates. The parameter is measured in milliseconds. | No | 300000 |

## CliMRJobLauncher Properties <a name="CliMRJobLauncher-Properties"></a>
There are no configuration parameters specific to CliMRJobLauncher. This class is used to launch Gobblin jobs on Hadoop from the command line, the jobs are not scheduled. Common properties are set using the `--sysconfig` option when launching jobs via the command line. For more information on how to set the configuration parameters for jobs launched through the command line, check out the [Deployment](Gobblin-Deployment) page.
  
## AzkabanJobLauncher Properties <a name="AzkabanJobLauncher-Properties"></a>
There are no configuration parameters specific to AzkabanJobLauncher. This class is used to schedule Gobblin jobs on Azkaban. Common properties can be set through Azkaban by creating a `.properties` file, check out the [Azkaban Documentation](http://azkaban.github.io/) for more information. For more information on how to set the configuration parameters for jobs scheduled through the Azkaban, check out the [Deployment](Gobblin-Deployment) page.

# Job Type Properties <a name="Job-Type-Properties"></a>
## Common Job Type Properties <a name="Common-Job-Type-Properties"></a>
| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `launcher.type` | Job launcher type; one of LOCAL, MAPREDUCE, YARN. LOCAL mode runs on a single machine (LocalJobLauncher), MAPREDUCE runs on a Hadoop cluster (MRJobLauncher), and YARN runs on a YARN cluster (not implemented yet). | No | LOCAL |

## LocalJobLauncher Properties <a name="LocalJobLauncher-Properties"></a>
There are no configuration parameters specific to LocalJobLauncher. The LocalJobLauncher will launch a Hadoop job on a single machine. If launcher.type is set to LOCAL then this class will be used to launch the job.
Properties required by the MRJobLauncher class.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `framework.jars` | Comma-separated list of jars the Gobblin framework depends on. These jars will be added to the classpath of the job, and to the classpath of any containers the job launches. | No | None |
| `job.jars` | Comma-separated list of jar files the job depends on. These jars will be added to the classpath of the job, and to the classpath of any containers the job launches. | No | None |
| `job.hdfs.jars` | Comma-separated list of jar files the job depends on located in HDFS. These jars will be added to the classpath of the job, and to the classpath of any containers the job launches. | No | None |
| `job.local.files` | Comma-separated list of local files the job depends on. These files will be available to any map tasks that get launched via the DistributedCache. | No | None |
| `job.hdfs.files` | Comma-separated list of files on HDFS the job depends on. These files will be available to any map tasks that get launched via the DistributedCache. | No | None |

## MRJobLauncher Properties <a name="MRJobLauncher-Properties"></a>

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `mr.job.root.dir` | Working directory for a Gobblin Hadoop MR job. Gobblin uses this to write intermediate data, such as the workunit state files that are used by each map task. This has to be a path on HDFS. | Yes | None |
| `mr.job.max.mappers` | Maximum number of mappers to use in a Gobblin Hadoop MR job. If no explicit limit is set then a map task for each workunit will be launched. If the value of this properties is less than the number of workunits created, then each map task will run multiple tasks. | No | None |
| `mr.include.task.counters` | Whether to include task-level counters in the set of counters reported as Hadoop counters. Hadoop imposes a system-level limit (default to 120) on the number of counters, so a Gobblin MR job may easily go beyond that limit if the job has a large number of tasks and each task has a few counters. This property gives users an option to not include task-level counters to avoid going over that limit. | Yes | False | 

# Retry Properties <a name="Retry-Properties"></a>
Properties that control how tasks and jobs get retried on failure.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `workunit.retry.enabled` | Whether retries of failed work units across job runs are enabled or not. | No | True |
| `workunit.retry.policy` | Work unit retry policy, can be one of {always, never, onfull, onpartial}. | No | always |
| `task.maxretries` | Maximum number of task retries. A task will be re-tried this many times before it is considered a failure. | No | 5 |
| `task.retry.intervalinsec` | Interval in seconds between task retries. The interval increases linearly with each retry. For example, if the first interval is 300 seconds, then the second one is 600 seconds, etc. | No | 300 |
| `job.max.failures` | Maximum number of failures before an alert email is triggered. | No | 1 |

# Task Execution Properties <a name="Task-Execution-Properties"></a>
These properties control how tasks get executed for a job. Gobblin uses thread pools in order to executes the tasks for a specific job. In local mode there is a single thread pool per job that executes all the tasks for a job. In MR mode there is a thread pool for each map task (or container), and all Gobblin tasks assigned to that mapper are executed in that thread pool.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `taskexecutor.threadpool.size` | Size of the thread pool used by task executor for task execution. Each task executor will spawn this many threads to execute any Tasks that is has been allocated. | No | 10 |
| `tasktracker.threadpool.coresize` | Core size of the thread pool used by task tracker for task state tracking and reporting. | No | 10 |
| `tasktracker.threadpool.maxsize` | Maximum size of the thread pool used by task tracker for task state tracking and reporting. | No | 10 |
| `taskretry.threadpool.coresize` | Core size of the thread pool used by the task executor for task retries. | No | 2 |
| `taskretry.threadpool.maxsize` | Maximum size of the thread pool used by the task executor for task retries. | No | 2 |
| `task.status.reportintervalinms` | Task status reporting interval in milliseconds. | No | 30000 |

# State Store Properties <a name="State-Store-Properties"></a>
| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `state.store.dir` | Root directory where job and task state files are stored. The state-store is used by Gobblin to track state between different executions of a job. All state-store files will be written to this directory. | Yes | None |
| `state.store.fs.uri` | File system URI for file-system-based state stores. | No | file:/// |

# Metrics Properties <a name="Metrics-Properties"></a>

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `metrics.enabled` | Whether metrics collecting and reporting are enabled or not. | No | True |
| `metrics.report.interval` | Metrics reporting interval in milliseconds. | No | 60000 |
| `metrics.log.dir` | The directory where metric files will be written to. | No | None |
| `metrics.reporting.file.enabled` | A boolean indicating whether or not metrics should be reported to a file. | No | True |
| `metrics.reporting.jmx.enabled` | A boolean indicating whether or not metrics should be exposed via JMX. | No | False |

# Email Alert Properties <a name="Email-Alert-Properties"></a>
| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `email.alert.enabled` | Whether alert emails are enabled or not. Email alerts are only sent out when jobs fail consecutively job.max.failures number of times. | No | False |
| `email.notification.enabled` | Whether job completion notification emails are enabled or not. Notification emails are sent whenever the job completes, regardless of whether it failed or not. | No | False |
| `email.host` | Host name of the email server. | Yes, if email notifications or alerts are enabled. | None |
| `email.smtp.port` | SMTP port number. | Yes, if email notifications or alerts are enabled. | None |
| `email.user` | User name of the sender email account. | No | None |
| `email.password` | User password of the sender email account. | No | None |
| `email.from` | Sender email address. | Yes, if email notifications or alerts are enabled. | None |
| `email.tos` | Comma-separated list of recipient email addresses. | Yes, if email notifications or alerts are enabled. | None |

# Source Properties <a name="Source-Properties"></a>
## Common Source Properties <a name="Common-Source-Properties"></a>
These properties are common properties that are used among different Source implementations. Depending on what source class is being used, these parameters may or may not be necessary. These parameters are not tied to a specific source, and thus can be used in new source classes.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `source.class` | Fully qualified name of the Source class. For example, `org.apache.gobblin.example.wikipedia.WikipediaSource` | Yes | None |
| `source.entity` | Name of the source entity that needs to be pulled from the source. The parameter represents a logical grouping of data that needs to be pulled from the source. Often this logical grouping comes in the form a database table, a source topic, etc. In many situations, such as when using the QueryBasedExtractor, it will be the name of the table that needs to pulled from the source. | Required for QueryBasedExtractors, FileBasedExtractors. | None |
| `source.timezone` | Timezone of the data being pulled in by the extractor. Examples include "PST" or "UTC". | Required for QueryBasedExtractors | None |
| `source.max.number.of.partitions` | Maximum number of partitions to split this current run across. Only used by the QueryBasedSource and FileBasedSource. | No | 20 |
| `source.skip.first.record` | True if you want to skip the first record of each data partition. Only used by the FileBasedExtractor. | No | False |
| `extract.namespace` | Namespace for the extract data. The namespace will be included in the default file name of the outputted data. | No | None |
| `source.conn.use.proxy.url` | The URL of the proxy to connect to when connecting to the source. This parameter is only used for SFTP and REST sources. | No | None |
| `source.conn.use.proxy.port` | The port of the proxy to connect to when connecting to the source. This parameter is only used for SFTP and REST sources. | No | None |
| `source.conn.username` | The username to authenticate with the source. This is parameter is only used for SFTP and JDBC sources. | No | None |
| `source.conn.password` | The password to use when authenticating with the source. This is parameter is only used for JDBC sources. | No | None |
| `source.conn.host` | The name of the host to connect to. | Required for SftpExtractor, MySQLExtractor, OracleExtractor, SQLServerExtractor and TeradataExtractor. | None |
| `source.conn.rest.url` | URL to connect to for REST requests. This parameter is only used for the Salesforce source. | No | None |
| `source.conn.version` | Version number of communication protocol. This parameter is only used for the Salesforce source. | No | None |
| `source.conn.timeout` | The timeout set for connecting to the source in milliseconds. | No | 500000 |
| `source.conn.port` | The value of the port to connect to. | Required for SftpExtractor, MySQLExtractor, OracleExtractor, SQLServerExtractor and TeradataExtractor. | None |
| `source.conn.sid` | The Oracle System ID (SID) that identifies the database to connect to. | Required for OracleExtractor. | None |
| `extract.table.name` | Table name in Hadoop which is different table name in source. | No | Source table name  |
| `extract.is.full` | True if this pull should treat the data as a full dump of table from the source, false otherwise. | No | false |
| `extract.delta.fields` | List of columns that will be used as the delta field for the data. | No | None |
| `extract.primary.key.fields ` | List of columns that will be used as the primary key for the data. | No | None |
| `extract.pull.limit` | This limits the number of records read by Gobblin. In Gobblin's extractor the readRecord() method is expected to return records until there are no more to pull, in which case it runs null. This parameter limits the number of times readRecord() is executed. This parameter is useful for pulling a limited sample of the source data for testing purposes. | No | Unbounded |
| `extract.full.run.time` | TODO | TODO | TODO |


## Distcp CopySource Properties <a name="Distcp-CopySource-Properties"></a>

 Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `gobblin.copy.simulate` | Will perform copy file listing but doesn't execute actual copy. | No | False |
| `gobblin.copy.includeEmptyDirectories` | Whether to include empty directories from the source in the copy. | No | False

### RecursiveCopyableDataset Properties <a name="RecursiveCopyableDataset-Properties"></a>
| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `gobblin.copy.recursive.deleteEmptyDirectories` | Whether to delete newly empty directories found, up to the dataset root.| No | False |
| `gobblin.copy.recursive.delete` | Whether to delete files in the target that don't exist in the source. | No | False | 
| `gobblin.copy.recursive.update` | Will update files that are different between the source and target, and skip files already in the target. | No | False |

### DistcpFileSplitter Properties <a name="DistcpFileSplitter-Properties"></a>
| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `gobblin.copy.split.enabled` | Will split files into block level granularity work units, which can be copied independently, then merged back together before publishing. To actually achieve splitting, the max split size property also needs to be set. | No | False| 
| `gobblin.copy.file.max.split.size` | If splitting is enabled, the split size (in bytes) for the block level work units is calculated based on rounding down the value of this property to the nearest integer multiple of the block size. If the value of this property is less than the block size, it gets adjusted up. | No | Long.MAX_VALUE

### WorkUnitBinPacker Properties <a name="WorkUnitBinPacker-Properties"></a>
| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `gobblin.copy.binPacking.maxSizePerBin` | Limits the maximum weight that can be packed into a multi work unit produced from bin packing. A value of 0 means packing is not done. | No | 0 |
| `gobblin.copy.binPacking.maxWorkUnitsPerBin` | Limits the maximum number/amount of work units that can be packed into a multi work unit produced from bin packing. | No | 50 |

## QueryBasedExtractor Properties <a name="QueryBasedExtractor-Properties"></a>
The following table lists the query based extractor configuration properties.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `source.querybased.watermark.type`  | The format of the watermark that is used when extracting data from the source. Possible types are timestamp, date, hour, simple. | Yes | timestamp | 
| `source.querybased.start.value`  | Value for the watermark to start pulling data from, also the default watermark if the previous watermark cannot be found in the old task states. | Yes | None |
| `source.querybased.partition.interval`  | Number of hours to pull in each partition. | No | 1 | 
| `source.querybased.hour.column`  | Delta column with hour for hourly extracts (Ex: hour_sk) | No | None |
| `source.querybased.skip.high.watermark.calc`  | If it is true, skips high watermark calculation in the source and it will use partition higher range as high watermark instead of getting it from source. | No | False | 
| `source.querybased.query`  | The query that the extractor should execute to pull data. | No | None |
| `source.querybased.hourly.extract`  | True if hourly extract is required. | No | False | 
| `source.querybased.extract.type`  | "snapshot" for the incremental dimension pulls. "append_daily", "append_hourly" and "append_batch" for the append data append_batch for the data with sequence numbers as watermarks | No | None |
| `source.querybased.end.value`  | The high watermark which this entire job should pull up to. If this is not specified, pull entire data from the table | No | None |
| `source.querybased.append.max.watermark.limit`  | max limit of the high watermark for the append data.  CURRENT_DATE - X CURRENT_HOUR - X where X>=1 | No | CURRENT_DATE for daily extract CURRENT_HOUR for hourly extract | 
| `source.querybased.is.watermark.override`  | True if this pull should override previous watermark with start.value and end.value. False otherwise. | No | False | 
| `source.querybased.low.watermark.backup.secs`  | Number of seconds that needs to be backup from the previous high watermark. This is to cover late data.  Ex: Set to 3600 to cover 1 hour late data. | No | 0 | 
| `source.querybased.schema`  | Database name | No | None |
| `source.querybased.is.specific.api.active`  | True if this pull needs to use source specific apis instead of standard protocols.  Ex: Use salesforce bulk api instead of rest api | No | False | 
| `source.querybased.skip.count.calc` | A boolean, if true then the QueryBasedExtractor will skip the source count calculation. | No | False | 
| `source.querybased.fetch.size` | This parameter is currently only used in JDBCExtractor. The JDBCExtractor will process this many number of records from the JDBC ResultSet at a time. It will then take these records and return them to the rest of the Gobblin flow so that they can get processed by the rest of the Gobblin components. | No  | 1000 |
| `source.querybased.is.metadata.column.check.enabled` | When a query is specified in the configuration file, it is possible a user accidentally adds in a column name that does not exist on the source side. By default, this parameter is set to false, which means that if a column is specified in the query and it does not exist in the source data set, Gobblin will just skip over that column. If it is set to true, Gobblin will actually take the config specified column and check to see if it exists in the source data set. If it doesn't exist then the job will fail. | No | False |
| `source.querybased.is.compression.enabled` | A boolean specifying whether or not compression should be enabled when pulling data from the source. This parameter is only used for MySQL sources. If set to true, the MySQL will send compressed data back to the source. | No | False |
| `source.querybased.jdbc.resultset.fetch.size` | The number of rows to pull through JDBC at a time. This is useful when the JDBC ResultSet is too big to fit into memory, so only "x" number of records will be fetched at a time. | No | 1000 |

### JdbcExtractor Properties <a name="JdbcExtractor-Properties"></a>
The following table lists the jdbc based extractor configuration properties.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `source.conn.driver` | The fully qualified path of the JDBC driver used to connect to the external source. | Yes | None |
| `source.column.name.case` | A enum specifying whether or not to convert the column names to a specific case before performing a query. Possible values are TOUPPER or TOLOWER. | No | NOCHANGE  |

## FileBasedExtractor Properties <a name="FileBasedExtractor-Properties"></a>
The following table lists the file based extractor configuration properties.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `source.filebased.data.directory` |  The data directory from which to pull data from. | Yes | None |
| `source.filebased.files.to.pull` |  A list of files to pull - this should be set in the Source class and the extractor will pull the specified files. | Yes   | None |
| `filebased.report.status.on.count` | The FileBasedExtractor will report it's status every time it processes the number of records specified by this parameter. The way it reports status is by logging out how many records it has seen. | No | 10000 |  
| `source.filebased.fs.uri` | The URI of the filesystem to connect to. | Required for HadoopExtractor. | None |
| `source.filebased.preserve.file.name` | A boolean, if true then the original file names will be preserved when they are are written to the source. | No | False |
| `source.schema` | The schema of the data that will be pulled by the source. | Yes | None |

### SftpExtractor Properties <a name="SftpExtractor-Properties"></a>

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `source.conn.private.key` | File location of the private key used for key based authentication. This parameter is only used for the SFTP source. | Yes | None |
| `source.conn.known.hosts` | File location of the known hosts file used for key based authentication. | Yes | None |

# Converter Properties <a name="Converter-Properties"></a>
Properties for Gobblin converters.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `converter.classes` |  Comma-separated list of fully qualified names of the Converter classes. The order is important as the converters will be applied in this order. | No | None |

## CsvToJsonConverter Properties <a name="CsvToJsonConverter-Properties"></a>
This converter takes in text data separated by a delimiter (converter.csv.to.json.delimiter), and splits the data into a JSON format recognized by JsonIntermediateToAvroConverter.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `converter.csv.to.json.delimiter` | The regex delimiter between CSV based files, only necessary when using the CsvToJsonConverter - e.g. ",", "/t" or some other regex | Yes | None |

## JsonIntermediateToAvroConverter Properties <a name="JsonIntermediateToAvroConverter-Properties"></a>
This converter takes in JSON data in a specific schema, and converts it to Avro data.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `converter.avro.date.format` | Source format of the date columns for Avro-related converters. | No | None |
| `converter.avro.timestamp.format` | Source format of the timestamp columns for Avro-related converters. | No | None |
| `converter.avro.time.format` | Source format of the time columns for Avro-related converters. | No | None |
| `converter.avro.binary.charset` | Source format of the time columns for Avro-related converters. | No | UTF-8 |
| `converter.is.epoch.time.in.seconds` | A boolean specifying whether or not a epoch time field in the JSON object is in seconds or not. | Yes | None |
| `converter.avro.max.conversion.failures` | This converter is will fail for this many number of records before throwing an exception. | No | 0 |
| `converter.avro.nullify.fields.enabled` | Generate new avro schema by nullifying fields that previously existed but not in the current schema. | No | false |
| `converter.avro.nullify.fields.original.schema.path` | Path of the original avro schema which will be used for merging and nullify fields. | No | None |

## JsonStringToJsonIntermediateConverter Properties <a name="JsonStringToJsonIntermediateConverter-Properties"></a>
| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `gobblin.converter.jsonStringToJsonIntermediate.unpackComplexSchemas` | Parse nested JSON record using source.schema. | No | True |

## AvroFilterConverter Properties <a name="AvroFilterConverter-Properties"></a>
This converter takes in an Avro record, and filters out records by performing an equality operation on the value of the field specified by converter.filter.field and the value specified in converter.filter.value. It returns the record unmodified if the equality operation evaluates to true, false otherwise.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `converter.filter.field` | The name of the field in the Avro record, for which the converter will filter records on. | Yes | None |
| `converter.filter.value` | The value that will be used in the equality operation to filter out records. | Yes | None |

## AvroFieldRetrieverConverter Properties <a name="AvroFieldRetrieverConverter-Properties"></a>
This converter takes a specific field from an Avro record and returns its value.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `converter.avro.extractor.field.path` | The field in the Avro record to retrieve. If it is a nested field, then each level must be separated by a period. | Yes | None |

## AvroFieldsPickConverter Properties <a name="AvroFieldsPickConverter-Properties"></a>
Unlike AvroFieldRetriever, this converter takes multiple fields from Avro schema and convert schema and generic record.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `converter.avro.fields` | Comma-separted list of the fields in the Avro record. If it is a nested field, then each level must be separated by a period. | Yes | None |

## AvroToJdbcEntryConverter Properties <a name="AvroToJdbcEntryConverter-Properties"></a>
Converts Avro schema and generic record into Jdbc entry schema and data.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `converter.avro.jdbc.entry_fields_pairs` | Converts Avro field name(s) to fit for JDBC underlying data base. Input format is key value pairs of JSON array where key is avro field name and value is corresponding JDBC column name. | No | None |

# Fork Properties <a name="Fork-Properties"></a>
Properties for Gobblin's fork operator.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `fork.operator.class` |  Fully qualified name of the ForkOperator class. | No | `org.apache.gobblin.fork.IdentityForkOperator` |
| `fork.branches` |  Number of fork branches. | No | 1 |
| `fork.branch.name.${branch index}` |  Name of a fork branch with the given index, e.g., 0 and 1. | No | fork_${branch index}, e.g., fork_0 and fork_1. |

# Quality Checker Properties <a name="Quality-Checker-Properties"></a>

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `qualitychecker.task.policies` | Comma-separted list of fully qualified names of the TaskLevelPolicy classes that will run at the end of each Task. | No | None |
| `qualitychecker.task.policy.types` | OPTIONAL implies the corresponding class in qualitychecker.task.policies is optional and if it fails the Task will still succeed, FAIL implies that if the corresponding class fails then the Task will fail too. | No | OPTIONAL | 
| `qualitychecker.row.policies` | Comma-separted list of fully qualified names of the RowLevelPolicy classes that will run on each record. | No | None |
| `qualitychecker.row.policy.types` | OPTIONAL implies the corresponding class in qualitychecker.row.policies is optional and if it fails the Task will still succeed, FAIL implies that if the corresponding class fails then the Task will fail too, ERR_FILE implies that if the record does not pass the test then the record will be written to an error file. | No | OPTIONAL | 
| `qualitychecker.row.err.file` | The quality checker will write the current record to the location specified by this parameter, if the current record fails to pass the quality checkers specified by qualitychecker.row.policies; this file will only be written to if the quality checker policy type is ERR_FILE. | No | None |

# Writer Properties <a name="Writer-Properties"></a>
| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `writer.destination.type` | Writer destination type. Can be HDFS, KAFKA, MYSQL or TERADATA | No | HDFS | 
| `writer.output.format` | Writer output format; currently only Avro is supported. | No | AVRO | 
| `writer.fs.uri` | File system URI for writer output. | No | file:/// | 
| `writer.staging.dir` | Staging directory of writer output. All staging data that the writer produces will be placed in this directory, but all the data will be eventually moved to the writer.output.dir. | Yes | None |
| `writer.output.dir` | Output directory of writer output. All output data that the writer produces will be placed in this directory, but all the data will be eventually moved to the final directory by the publisher. | Yes | None |
| `writer.builder.class` | Fully qualified name of the writer builder class. | No | `org.apache.gobblin.writer.AvroDataWriterBuilder` |
| `writer.file.path` | The Path where the writer will write it's data. Data in this directory will be copied to it's final output directory by the DataPublisher. | Yes | None |
| `writer.file.name` | The name of the file the writer writes to. | Yes | part | 
| `writer.partitioner.class` | Partitioner used for distributing records into multiple output files. `writer.builder.class` must be a subclass of `PartitionAwareDataWriterBuilder`, otherwise Gobblin will throw an error.  | No | None (will not use partitioner) |
| `writer.buffer.size` |  Writer buffer size in bytes. This parameter is only applicable for the AvroHdfsDataWriter. | No | 4096 | 
| `writer.deflate.level` |  Writer deflate level. Deflate is a type of compression for Avro data. | No | 9 | 
| `writer.codec.type` |  This is used to specify the type of compression used when writing data out. Possible values are NOCOMPRESSION, DEFLATE, SNAPPY. | No | DEFLATE | 
| `writer.eager.initialization` | This is used to control the writer creation. If the value is set to true, writer is created before records are read. This means an empty file will be created even if no records were read. | No | False | 
| `writer.parquet.page.size` | The page size threshold | No | 1048576 |
| `writer.parquet.dictionary.page.size` | The block size threshold. | No | 134217728 |
| `writer.parquet.dictionary` | To turn dictionary encoding on. | No | true |
| `writer.parquet.validate` | To turn on validation using the schema. | No | false |
| `writer.parquet.version` | Version of parquet writer to use. Available versions are v1 and v2. | No | v1 |

# Data Publisher Properties <a name="Data-Publisher-Properties"></a>

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `data.publisher.type` |  The fully qualified name of the DataPublisher class to run. The DataPublisher is responsible for publishing task data once all Tasks have been completed. | Yes | None |
| `data.publisher.final.dir` |  The final output directory where the data should be published. | Yes | None |
| `data.publisher.replace.final.dir` | A boolean, if true and the the final output directory already exists, then the data will not be committed. If false and the final output directory already exists then it will be overwritten. | Yes | None |
| `data.publisher.final.name` | The final name of the file that is produced by Gobblin. By default, Gobblin already assigns a unique name to each file it produces. If that default name needs to be overridden then this parameter can be used. Typically, this parameter should be set on a per workunit basis so that file names don't collide. | No | None |
 
# Generic Properties <a name="Generic-Properties"></a>
These properties are used throughout multiple Gobblin components.

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `fs.uri` | Default file system URI for all file storage; over-writable by more specific configuration properties. | No | file:/// |

# FileBasedJobLock Properties <a name="FileBasedJobLock-Properties"></a>

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `job.lock.dir` | Directory where job locks are stored. Job locks are used by the scheduler to ensure two executions of a job do not run at the same time. If a job is scheduled to run, Gobblin will first check this directory to see if there is a lock file for the job. If there is one, it will not run the job, if there isn't one then it will run the job. | No | None |

# ZookeeperBasedJobLock Properties <a name="ZookeeperBasedJobLock-Properties"></a>

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `zookeeper.connection.string` | The connection string to the ZooKeeper cluster used to manage the lock. | No | localhost:2181 |
| `zookeeper.session.timeout.seconds` | The zookeeper session timeout. | No | 180 |
| `zookeeper.connection.timeout.seconds` | The zookeeper conection timeout. | No | 30 |
| `zookeeper.retry.backoff.seconds` | The amount of time in seconds to wait between retries.  This will increase exponentially when retries occur. | No | 1 |
| `zookeeper.retry.count.max` | The maximum number of times to retry. | No | 10 |
| `zookeeper.locks.acquire.timeout.milliseconds` | The amount of time in milliseconds to wait while attempting to acquire the lock. | No | 5000 |
| `zookeeper.locks.reaper.threshold.seconds` | The threshold in seconds that determines when a lock path can be deleted. | No | 300 |

# JDBC Writer properties <a name="JdbcWriter-Properties"></a>
Writer(and publisher) that writes to JDBC database. Please configure below two properties to use JDBC writer & publisher.

*  writer.builder.class=org.apache.gobblin.writer.JdbcWriterBuilder
*  data.publisher.type=org.apache.gobblin.publisher.JdbcPublisher

| Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `jdbc.publisher.database_name` | Destination database name | Yes | None | 
| `jdbc.publisher.table_name` | Destination table name | Yes | None | 
| `jdbc.publisher.replace_table` | Gobblin will replace the data in destination table. | No | false | 
| `jdbc.publisher.username` | User name to connect to destination database | Yes | None | 
| `jdbc.publisher.password` | Password to connect to destination database. Also, accepts encrypted password. | Yes | None | 
| `jdbc.publisher.encrypt_key_loc` | Location of a key to decrypt an encrypted password | No | None | 
| `jdbc.publisher.url` | Connection URL | Yes | None | 
| `jdbc.publisher.driver` | JDBC driver class | Yes | None |  
| `writer.staging.table` | User can pass staging table for Gobblin to use instead of Gobblin to create one. (e.g: For the user who does not have create table previlege can pass staging table for Gobblin to use). | No | None | 
| `writer.truncate.staging.table` | Truncate staging table if user passed their own staging table via "writer.staging.table". | No | false | 
| `writer.jdbc.batch_size` | Batch size for Insert operation | No | 30 | 
| `writer.jdbc.insert_max_param_size` | Maximum number of parameters for JDBC insert operation (for MySQL Writer). | No | 100,000 (MySQL limitation) | 
