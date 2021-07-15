Table of Contents
--------------------

[TOC]

Overview
--------------------
Gobblin provides the users a way of keeping tracking of executions of their jobs through the Job Execution History Store, which can be queried either directly if the implementation supports queries directly or through a Rest API. Note that using the Rest API needs the Job Execution History Server to be up and running. The Job Execution History Server will be discussed later. By default, writing to the Job Execution History Store is not enabled. To enable it, set configuration property `job.history.store.enabled` to `true`.

Information Recorded
--------------------------------
The Job Execution History Store stores various pieces of information of a job execution, including both job-level and task-level stats and measurements that are summarized below.

Job Execution Information
-------------------------------------------------
The following table summarizes job-level execution information the Job Execution History Store stores. 

|Information| Description|
|---------------------------------|----------------------|
|Job name|Gobblin job name.|
|Job ID|Gobblin job ID.|
|Start time|Start time in epoch time (of unit milliseconds) of the job in the local time zone.|
|End time|End time in epoch time (of unit milliseconds) of the job in the local time zone.|
|Duration|Duration of the job in milliseconds.|
|Job state|Running state of the job. Possible values are `PENDING`, `RUNNING`, `SUCCESSFUL`, `COMMITTED`, `FAILED`, `CANCELLED`.|
|Launched tasks|Number of launched tasks of the job.|
|Completed tasks|Number of tasks of the job that completed.|
|Launcher type|The type of the launcher used to launch and run the task.|
|Job tracking URL|This will be set to the MapReduce job URL if the Gobblin job is running on Hadoop MapReduce. This may also be set to the Azkaban job execution tracking URL if the job is running through Azkaban but not on Hadoop MapReduce. Otherwise, this will be empty.|
|Job-level metrics|Values of job-level metrics. Note that this data is not time-series based so the values will be overwritten on every update.|
|Job configuration properties|Job configuration properties used at runtime for job execution. Note that it may include changes made at runtime by the job.|

Task Execution Information
-------------------------------------------------
The following table summarizes task-level execution information the Job Execution History Store stores. 

|Information| Description|
|---------------------------------|----------------------|
|Task ID|Gobblin task ID.|
|Job ID|Gobblin job ID.|
|Start time|Start time in epoch time (of unit milliseconds) of the task in the local time zone.|
|End time|End time in epoch time (of unit milliseconds) of the task in the local time zone.|
|Duration|Duration of the task in milliseconds.|
|Task state|Running state of the task. Possible values are `PENDING`, `RUNNING`, `SUCCESSFUL`, `COMMITTED`, `FAILED`, `CANCELLED`.|
|Task failure exception|Exception message in case of task failure.|
|Low watermark|The low watermark of the task if avaialble.|
|High watermark|The high watermark of the task if available.|
|Extract namespace|The namespace of the `Extract`. An `Extract` is a concept describing the ingestion work of a job. This stores the value specified through the configuration property `extract.namespace`.|
|Extract name|The name of the `Extract`. This stores the value specified through the configuration property `extract.table.name`.|
|Extract type|The type of the `Extract`. This stores the value specified through the configuration property `extract.table.type`.|
|Task-level metrics|Values of task-level metrics. Note that this data is not time-series based so the values will be overwritten on every update.|
|Task configuration properties|Task configuration properties used at runtime for task execution. Note that it may include changes made at runtime by the task.|


Default Implementation
--------------------------------
The default implementation of the Job Execution History Store stores job execution information into a MySQL database in a few different tables. Specifically, the following tables are used and should be created before writing to the store is enabled. Checkout the MySQL [DDLs](https://github.com/apache/gobblin/tree/master/gobblin-metastore/src/main/resources/db/migration) of the tables for detailed columns of each table.

* Table `gobblin_job_executions` stores basic information about a job execution including the start and end times, job running state, number of launched and completed tasks, etc. 
* Table `gobblin_task_executions` stores basic information on task executions of a job, including the start and end times, task running state, task failure message if any, etc, of each task. 
* Table `gobblin_job_metrics` stores values of job-level metrics collected through the `JobMetrics` class. Note that this data is not time-series based and values of metrics are overwritten on every update to the job execution information. 
* Table `gobblin_task_metrics` stores values of task-level metrics collected through the `TaskMetrics` class. Again, this data is not time-series based and values of metrics are overwritten on updates.
* Table `gobblin_job_properties` stores the job configuration properties used at runtime for the job execution, which may include changes made at runtime by the job.
* Table `gobblin_task_properties` stores the task configuration properties used at runtime for task executions, which also may include changes made at runtime by the tasks.

To enable writing to the MySQL-backed Job Execution History Store, the following configuration properties (with sample values) need to be set:

```properties
job.history.store.url=jdbc:mysql://localhost/gobblin
job.history.store.jdbc.driver=com.mysql.jdbc.Driver
job.history.store.user=gobblin
job.history.store.password=gobblin
``` 


Rest Query API
--------------------------------

The Job Execution History Store Rest API supports three types of queries: query by job name, query by job ID, or query by extract name. The query type can be specified using the field `idType` in the query json object and can have one of the values `JOB_NAME`, `JOB_ID`, or `TABLE`. All three query types require the field `id` in the query json object, which should have a proper value as documented in the following table. 

|Query type|Query ID|
|---------------------------------|----------------------|
|JOB_NAME|Gobblin job name.|
|JOB_ID|Gobblin job ID.|
|TABLE|A json object following the `TABLE` schema shown below.|

```json
{
    "type": "record",
    "name": "Table",
    "namespace": "gobblin.rest",
    "doc": "Gobblin table definition",
    "fields": [
      {
          "name": "namespace",
          "type": "string",
          "optional": true,
          "doc": "Table namespace"
      },
      {
          "name": "name",
          "type": "string",
          "doc": "Table name"
      },
      {
          "name": "type",
          "type": {
              "name": "TableTypeEnum",
              "type": "enum",
              "symbols" : [ "SNAPSHOT_ONLY", "SNAPSHOT_APPEND", "APPEND_ONLY" ]
          },
          "optional": true,
          "doc": "Table type"
      }
    ]
}
```

For each query type, there are also some option fields that can be used to control the number of records returned and what should be included in the query result. The optional fields are summarized in the following table.

|Optional field|Type|Description|
|---------------------------------|----------------------|----------------------|
|`limit`|`int`|Limit on the number of records returned.|
|`timeRange`|`TimeRange`|The query time range. The schema of `TimeRange` is shown below.|
|`jobProperties`|`boolean`|This controls whether the returned record should include the job configuration properties.|
|`taskProperties`|`boolean`|This controls whether the returned record should include the task configuration properties.|

```json
{
    "type": "record",
    "name": "TimeRange",
    "namespace": "gobblin.rest",
    "doc": "Query time range",
    "fields": [
      {
          "name": "startTime",
          "type": "string",
          "optional": true,
          "doc": "Start time of the query range"
      },
      {
          "name": "endTime",
          "type": "string",
          "optional": true,
          "doc": "End time of the query range"
      },
      {
          "name": "timeFormat",
          "type": "string",
          "doc": "Date/time format used to parse the start time and end time"
      }
    ]
}
```

The API is built with [rest.li](http://www.rest.li), which generates documentation on compilation and can be found at `http://<hostname:port>/restli/docs`.

### Example Queries
*Fetch the 10 most recent job executions with a job name `TestJobName`*
```bash
curl "http://<hostname:port>/jobExecutions/idType=JOB_NAME&id.string=TestJobName&limit=10"
```

Job Execution History Server
--------------------------------
The Job Execution History Server is a Rest server for serving queries on the Job Execution History Store through the Rest API described above. The Rest endpoint URL is configurable through the following configuration properties (with their default values):
```properties
rest.server.host=localhost
rest.server.port=8080
```

**Note:** This server is started in the standalone deployment if configuration property `job.execinfo.server.enabled` is set to `true`.
