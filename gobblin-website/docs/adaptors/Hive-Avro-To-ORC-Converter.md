---
title: Hive Avro-To-Orc Converter
sidebar_label: Hive Avro-To-Orc Converter
---

# Getting Started

Gobblin provides ready to use adapters for converting data in [Avro](http://avro.apache.org/) to [ORC](https://orc.apache.org/). This page describes the steps to setup such a job.

<b>Note: The job requires Avro data to be registered in Hive.</b>

* Gobblin Avro to ORC job leverages [Hive](http://hive.apache.org/) for the conversion. Meaning, Gobblin does not read the Avro data record by record and convert each one of them to ORC, instead Gobblin executes hive queries to perform the conversion. This means that Avro data MUST be registred in hive for the converison to be possible. Below is a sample query.

<b>Example Conversion DDL</b>

```sql
INSERT OVERWRITE TABLE db_name_orc.table_orc
  PARTITION (year='2016')
          SELECT
              header.id,
              header.time,
              ... (more columns to select)
              ...
              ...
FROM db_name_avro.table_avro WHERE year='2016';
```

* Since Hive takes care of scaling the number of mappers/reducers required to perform the conversion, Gobblin does not run this job in MR mode. It runs in standalone mode.
* Each workunit converts a hive partition or a hive table (non partitioned tables).
* Each workunit/task executes one or more Hive DDLs.
* A gobblin task publishes data to a staging table first. The publisher moves data into the final table.
* The job supports schema evolution. Meaning any schema (compatible) changes on the Avro table are automatically made on the ORC table.
* By default publishing happens per dataset (dataset = table in this context). If a dataset fails, other datasets will still be published but the job will fail. The commit policy is configurable.
* Gobblin metrics is used to emit events when ORC data is published or when publish fails.


# Job Constructs

## Source and Extractor

Gobblin provides [`HiveSource`](https://github.com/apache/gobblin/blob/master/gobblin-data-management/src/main/java/org/apache/gobblin/data/management/conversion/hive/source/HiveSource.java) which is a generic source that connects to the hive metastore and creates `WorkUnits` for any Hive `Partitions` and `Tables` whitelisted. The [`HiveConvertExtractor`](https://github.com/apache/gobblin/blob/master/gobblin-data-management/src/main/java/org/apache/gobblin/data/management/conversion/hive/extractor/HiveConvertExtractor.java) is a Gobblin `Extractor` to extracts work for Avro to ORC conversion.

The `HiveSource` uses the `HiveDatasetFinder` to find all hive tables and partitions that satisfy a whitelist. For each table/partition it creates a workunit is the `updateTime` is greater than the `lowWatermark`. By default a [`PartitionLevelWatermarker`](https://github.com/apache/gobblin/blob/master/gobblin-data-management/src/main/java/org/apache/gobblin/data/management/conversion/hive/watermarker/PartitionLevelWatermarker.java) is used. This watermarker tracks watermarks for every partition of the table. Gobblin also provides a [`TableLevelWatermarker`](https://github.com/apache/gobblin/blob/master/gobblin-data-management/src/main/java/org/apache/gobblin/data/management/conversion/hive/watermarker/TableLevelWatermarker.java) that keeps one watermark per table.

The `HiveConvertExtractor` builds `QueryBasedHiveConversionEntity`s. The extractor makes necessary calls to the Hive Metastore to get table/partition metadata. The metadata is then wrapped into a `QueryBasedHiveConversionEntity`.

## Converter

The converter builds the Hive DDLs/DMLs required to perform the Avro to ORC conversion. Gobblin supports conversion of Avro to both flattened ORC and nested ORC.
The abstract converter [`AbstractAvroToOrcConverter`](https://github.com/apache/gobblin/blob/master/gobblin-data-management/src/main/java/org/apache/gobblin/data/management/conversion/hive/converter/AbstractAvroToOrcConverter.java) builds DDLs/DMLs for any destination ORC format. Concrete subclass [`HiveAvroToFlattenedOrcConverter`](https://github.com/apache/gobblin/blob/master/gobblin-data-management/src/main/java/org/apache/gobblin/data/management/conversion/hive/converter/HiveAvroToFlattenedOrcConverter.java) provides the configurations required for Avro to flattened ORC conversion and [`HiveAvroToNestedOrcConverter`](https://github.com/apache/gobblin/blob/master/gobblin-data-management/src/main/java/org/apache/gobblin/data/management/conversion/hive/converter/HiveAvroToNestedOrcConverter.java) provides the configurations required for Avro to nested ORC conversion. In the job configurations, both converters can be chained to perform flattend and nested ORC conversion in the same job. Each converter can also be used independent of other.

The converter builds the following different DDLs/DMLs

* Create staging table DDL - ORC data is written to a staging table first. The publisher then publishes them to the final ORC table. These DDLs are to create the staging table. A staging table looks like `<orc_db_name>.<orc_table_name>_staging_<timestamp>`
* Create staging partition DDL - Similar to staging table but for a partition
* Conversion staging DML - This is the DML to select rows from Avro source table and insert them into the ORC staging table
* Create final table DDL (Optional) - This is the final ORC destination table. Creates the destination table is it does not exist
* Evolve final table DDLs (Optional) - Populate the schema evolution queries if required
* Drop partitions if exist in final table - DDL to drop a partition on destination if it already exists.
* Create final partition DDL - Create the ORC partition
* Drop staging table DDL - Cleanup the staging table after data is published from staging to final tables

## Writer

The writer in this context executes the Hive DDLs/DMLs generated by the converter. [`HiveQueryExecutionWriter`](https://github.com/apache/gobblin/blob/master/gobblin-data-management/src/main/java/org/apache/gobblin/data/management/conversion/hive/writer/HiveQueryExecutionWriter.java) uses Hive JDBC connector to execute the DDLs. The DDLs write ORC data into staging tables. After the writer has completed `HiveQueryExecutionWriter#write()`, ORC data will be available in the staging tables.


## Publisher

The publisher [`HiveConvertPublisher`](https://github.com/apache/gobblin/blob/master/gobblin-data-management/src/main/java/org/apache/gobblin/data/management/conversion/hive/publisher/HiveConvertPublisher.java) executes hive DDLs to publish staging ORC tables to final ORC tables. The publisher also cleans up staging tables.
By default publishing happens per dataset (dataset = table in this context). If a dataset fails, other datasets will still be published but the job will fail. The commit policy is configurable.


# Job Config Properties

These are some of the job config properties used by `HiveAvroToOrcSource` and `HiveConvertExtractor`.

| Configuration key | Description | Example value |
| --- | --- | --- |
|  hive.dataset.whitelist  | Avro hive databases, tables to be converted |   <ol> <li>db1 -> any table under db1 passes.</li><li>db1.table1 -> only db1.table1 passes.</li><li>db1.table* -> any table under db1 whose name satisfies the pattern table* passes.</li><li>db* -> all tables from all databases whose names satisfy the pattern db* pass.</li><li>db*.table* -> db and table must satisfy the patterns db* and table* respectively </li><li>db1.table1,db2.table2 -> combine expressions for different databases with comma.</li><li>db1.table1&#124;table2 -> combine expressions for same database with "&#124;".</li></ol>   |
| hive.dataset.blacklist | Avro hive databases, tables not to converted |  Same as hive.dataset.whitelist examples  |
|  gobblin.runtime.root.dir  |  Root dir for gobblin state store, staging, output etc.  |  /jobs/user/avroToOrc  |
|  hive.source.maximum.lookbackDays  |  Partitions older than this value will not be processed.<br/> The default value is set to 3. <br/> <br/>So if an Avro partition older than 3 days gets modified, the job will not convert the new changes.  | 3 |
|  hive.source.watermarker.class  |  The type of watermark to use. Watermark can be per partition or per table. The default is `gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker`  |  gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker <br/> <br/> gobblin.data.management.conversion.hive.watermarker.TableLevelWatermarker  |
|  taskexecutor.threadpool.size  |  Maximum number of parallel conversion hive queries to run. <br/> <br/>This is the standard gobblin property to control the number of parallel tasks (threads). This is set to a default of 50 because each task queries the hive metastore. So this property also limits the number of parallel metastore connections  | 50 |
|  hive.conversion.avro.flattenedOrc.destination.dbName  | Name of the ORC database |  $DB is the Avro database name.<br/> E.g. If avro database name is tracking, $DB will be resolved at runtime to tracking.  <ul><li>Setting the value to "$DB_column" will result in a ORC table name of tracking_column</li></ul>   |
|  hive.conversion.avro.flattenedOrc.destination.tableName  |  Name of the ORC table |  $TABLE is the Avro table name.<br/> E.g. If avro table name is LogEvent, $TABLE will be resolved at runtime to LogEvent.  <ul><li>Setting the value of this property to "$TABLE" will cause the ORC table name to be same as Avro table name.</li> <li>Setting the value to "$TABLE_orc" will result in a ORC table name of LogEvent_orc</li></ul>   |
|  hive.conversion.avro.flattenedOrc.destination.dataPath  | Location on HDFS where ORC data is published | /events_orc/$DB/$TABLE |
|  hive.conversion.avro.flattenedOrc.evolution.enabled  | Decides if schema evolution is enabled | true/false |
|  hive.conversion.avro.flattenedOrc.hiveRuntime.*  |  Additional hive properties to be set while executing the conversion DDL. Prefix any hive standard properties with this key  |  hive.conversion.avro.flattenedOrc.hiveRuntime.mapred.map.tasks=10  |
|  hive.conversion.avro.destinationFormats  | A comma separated list of destination formats. Currently supports nestedOrc and flattenedOrc | flattenedOrc,nestedOrc |

# Metrics and Events

SLA event is published every time an Avro partition/table is converted to ORC. Each SLA event has the following metadata.

```json
{
    ## Publish timestamp
    "timestamp" : "1470229945441",
    "namespace" : "gobblin.hive.conversion",
    "name" : "gobblin.hive.conversion.ConversionSuccessful",
    "metadata" : {

        ## Azkaban metadata (If running on Azkaban)
        "azkabanExecId": "880060",
        "azkabanFlowId": "azkaban_flow_name",
        "azkabanJobId": "azkaban_job_name",
        "azkabanProjectName": "azkaban_project_name",
        "jobId": "job_AvroToOrcConversion_1470227416023",
        "jobName": "AvroToOrcConversion",

        ## Dataset and Partition metadata
        "datasetUrn": "events@logevent",
        "sourceDataLocation": "hdfs://<host>:<port>/events/LogEvent/2016/08/03/04",
        "partition": "datepartition=2016-08-03-04",
        "schemaEvolutionDDLNum": "0",

        ## Begin and End time metadata for each phase
        "beginConversionDDLExecuteTime": "1470227453370",
        "beginDDLBuildTime": "1470227452382",
        "beginGetWorkunitsTime": "1470227428136",
        "beginPublishDDLExecuteTime": "1470229944141",
        "endConversionDDLExecuteTime": "1470227928486",
        "endDDLBuildTime": "1470227452382",
        "endPublishDDLExecuteTime": "1470229945440",
        "originTimestamp": "1470227446703",
        "previousPublishTs": "1470223843230",
        "upstreamTimestamp": "1470226593984",
        "workunitCreateTime": "1470227446703"

        ## Gobblin metrics metadata
        "class": "org.apache.gobblin.data.management.conversion.hive.publisher.HiveConvertPublisher",
        "metricContextID": "20bfb2a2-0592-4f53-9259-c8ee125f90a8",
        "metricContextName": "org.apache.gobblin.data.management.conversion.hive.publisher.HiveConvertPublisher.781426901",
    }
}
```

The diagram below describes timestamps captured in the SLA event.
![Event metadata description](../../static/img/Avro-to-Orc-timeline.jpg)

# Sample Job

```properties
# Avro hive databases and tables to convert
hive.dataset.whitelist=events.LogEvent|LoginEvent

data.publisher.type=org.apache.gobblin.data.management.conversion.hive.publisher.HiveConvertPublisher
source.class=org.apache.gobblin.data.management.conversion.hive.source.HiveAvroToOrcSource
writer.builder.class=org.apache.gobblin.data.management.conversion.hive.writer.HiveQueryWriterBuilder
converter.classes=org.apache.gobblin.data.management.conversion.hive.converter.HiveAvroToFlattenedOrcConverter,org.apache.gobblin.data.management.conversion.hive.converter.HiveAvroToNestedOrcConverter

hive.dataset.finder.class=org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDatasetFinder

# Only flattened orc is enabled
hive.conversion.avro.destinationFormats=flattenedOrc
hive.conversion.avro.flattenedOrc.destination.dataPath=/events_orc/

# Avro table name _orc
hive.conversion.avro.flattenedOrc.destination.tableName=$TABLE_orc

# Same as Avro table name
hive.conversion.avro.flattenedOrc.destination.dbName=$DB
hive.conversion.avro.flattenedOrc.evolution.enabled=true
hive.conversion.avro.flattenedOrc.source.dataPathIdentifier=daily,hourly

# No host and port required. Hive starts an embedded hiveserver2
hiveserver.connection.string=jdbc:hive2://

## Maximum lookback
hive.source.maximum.lookbackDays=3

## Gobblin standard properties ##
task.maxretries=1
taskexecutor.threadpool.size=75
workunit.retry.enabled=true


# Gobblin framework locations
mr.job.root.dir=/jobs/working
state.store.dir=/jobs/state_store
writer.staging.dir=/jobs/writer_staging
writer.output.dir=/jobs/writer_output

# Metrics
metrics.enabled=true
metrics.reporting.kafka.enabled=true
metrics.reporting.kafka.format=avro
metrics.reporting.kafka.avro.use.schema.registry=true
metrics.reporting.kafka.topic.metrics=MetricReport

launcher.type=LOCAL
classpath=lib/*
```
