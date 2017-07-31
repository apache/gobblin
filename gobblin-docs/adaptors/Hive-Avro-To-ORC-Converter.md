Table of Contents
--------------------

[TOC]

# Getting Started

Gobblin provides ready to use adapters for converting data in [Avro](http://avro.apache.org/) to [ORC](https://orc.apache.org/). This page describes the steps to setup such a job.

<b>Note: The job requires Avro data to be registered in Hive.</b>

* Gobblin Avro to ORC job leverages [Hive](http://hive.apache.org/) for the conversion. Meaning, Gobblin does not read the Avro data record by record and convert each one of them to ORC, instead Gobblin executes hive queries to perform the conversion. This means that Avro data MUST be registred in hive for the converison to be possible. Below is a sample query.

<b>Example Conversion DDL</b>

```
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

Gobblin provides [`HiveSource`](https://github.com/linkedin/gobblin/blob/master/gobblin-data-management/src/main/java/gobblin/data/management/conversion/hive/source/HiveSource.java) which is a generic source that connects to the hive metastore and creates `WorkUnits` for any Hive `Partitions` and `Tables` whitelisted. The [`HiveConvertExtractor`](https://github.com/linkedin/gobblin/blob/master/gobblin-data-management/src/main/java/gobblin/data/management/conversion/hive/extractor/HiveConvertExtractor.java) is a Gobblin `Extractor` to extracts work for Avro to ORC conversion.

The `HiveSource` uses the `HiveDatasetFinder` to find all hive tables and partitions that satisfy a whitelist. For each table/partition it creates a workunit is the `updateTime` is greater than the `lowWatermark`. By default a [`PartitionLevelWatermarker`](https://github.com/linkedin/gobblin/blob/master/gobblin-data-management/src/main/java/gobblin/data/management/conversion/hive/watermarker/PartitionLevelWatermarker.java) is used. This watermarker tracks watermarks for every partition of the table. Gobblin also provides a [`TableLevelWatermarker`](https://github.com/linkedin/gobblin/blob/master/gobblin-data-management/src/main/java/gobblin/data/management/conversion/hive/watermarker/TableLevelWatermarker.java) that keeps one watermark per table.

The `HiveConvertExtractor` builds `QueryBasedHiveConversionEntity`s. The extractor makes necessary calls to the Hive Metastore to get table/partition metadata. The metadata is then wrapped into a `QueryBasedHiveConversionEntity`.

## Converter

The converter builds the Hive DDLs/DMLs required to perform the Avro to ORC conversion. Gobblin supports conversion of Avro to both flattened ORC and nested ORC.
The abstract converter [`AbstractAvroToOrcConverter`](https://github.com/linkedin/gobblin/blob/master/gobblin-data-management/src/main/java/gobblin/data/management/conversion/hive/converter/AbstractAvroToOrcConverter.java) builds DDLs/DMLs for any destination ORC format. Concrete subclass [`HiveAvroToFlattenedOrcConverter`](https://github.com/linkedin/gobblin/blob/master/gobblin-data-management/src/main/java/gobblin/data/management/conversion/hive/converter/HiveAvroToFlattenedOrcConverter.java) provides the configurations required for Avro to flattened ORC conversion and [`HiveAvroToNestedOrcConverter`](https://github.com/linkedin/gobblin/blob/master/gobblin-data-management/src/main/java/gobblin/data/management/conversion/hive/converter/HiveAvroToNestedOrcConverter.java) provides the configurations required for Avro to nested ORC conversion. In the job configurations, both converters can be chained to perform flattend and nested ORC conversion in the same job. Each converter can also be used independent of other.

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

The writer in this context executes the Hive DDLs/DMLs generated by the converter. [`HiveQueryExecutionWriter`](https://github.com/linkedin/gobblin/blob/master/gobblin-data-management/src/main/java/gobblin/data/management/conversion/hive/writer/HiveQueryExecutionWriter.java) uses Hive JDBC connector to execute the DDLs. The DDLs write ORC data into staging tables. After the writer has completed `HiveQueryExecutionWriter#write()`, ORC data will be available in the staging tables.


## Publisher

The publisher [`HiveConvertPublisher`](https://github.com/linkedin/gobblin/blob/master/gobblin-data-management/src/main/java/gobblin/data/management/conversion/hive/publisher/HiveConvertPublisher.java) executes hive DDLs to publish staging ORC tables to final ORC tables. The publisher also cleans up staging tables.
By default publishing happens per dataset (dataset = table in this context). If a dataset fails, other datasets will still be published but the job will fail. The commit policy is configurable.


# Job Config Properties

These are some of the job config properties used by `KafkaSource` and `KafkaExtractor`.

<table style="table-layout: fixed; width: 100%">
  <col width="20%">
  <col width="40%">
  <col width="40%">
   <tr>
      <th>Configuration key</th>
      <th>Description</th>
      <th>Example value</th>
   </tr>
   <tr>
      <td style="word-wrap: break-word">
         <p>hive.dataset.whitelist</p>
      </td>
      <td style="word-wrap: break-word">Avro hive databases, tables to be converted</td>
      <td style="word-wrap: break-word">
         <ol>
            <li >db1 -&gt; any table under db1 passes.</li>
            <li >db1.table1 -&gt; only db1.table1 passes.</li>
            <li>db1.table* -&gt; any table under db1 whose name satisfies the pattern table* passes.</li>
            <li>db* -&gt; all tables from all databases whose names satisfy the pattern db* pass.</li>
            <li>db*.table* -&gt; db and table must satisfy the patterns db* and table* respectively</li>
            <li>db1.table1,db2.table2 -&gt; combine expressions for different databases with comma.</li>
            <li>db1.table1|table2 -&gt; combine expressions for same database with &quot;|&quot;.</li>
         </ol>
      </td>
   </tr>
   <tr>
      <td style="word-wrap: break-word">hive.dataset.blacklist</td>
      <td style="word-wrap: break-word">Avro hive databases, tables not to converted</td>
      <td style="word-wrap: break-word">
         <p >Same as hive.dataset.whitelist examples</p>
      </td>
   </tr>
   <tr>
      <td style="word-wrap: break-word">
         <p >gobblin.runtime.root.dir</p>
      </td>
      <td style="word-wrap: break-word">
         <p >Root dir for <span class="s1">gobblin</span> state store, staging, output etc.</p>
      </td>
      <td style="word-wrap: break-word">
         <p >/jobs/user/avroToOrc</p>
      </td>
   </tr>
   <tr>
      <td style="word-wrap: break-word">
         <p >hive.source.maximum.lookbackDays</p>
      </td>
      <td style="word-wrap: break-word">
         <p>Partitions older than this value will not be processed. The default value is set to 3.</p>
         <p>So if an Avro partition older than 3 days gets modified, the job will not convert the new changes.</p>
      </td>
      <td style="word-wrap: break-word">3</td>
   </tr>
   <tr>
      <td style="word-wrap: break-word">
         <p >hive.source.watermarker.class</p>
      </td>
      <td style="word-wrap: break-word">
         The type of watermark to use. Watermark can be per partition or per table. The default is
         <p ><span class="s1">gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker</span></p>
      </td>
      <td style="word-wrap: break-word">
         <p><span>gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker</span></p>
         <p><span><span>gobblin.data.management.conversion.hive.watermarker.TableLevelWatermarker</span></span></p>
      </td>
   </tr>
   <tr>
      <td style="word-wrap: break-word">
         <p >taskexecutor.threadpool.size</p>
      </td>
      <td style="word-wrap: break-word">
         <p>Maximum number of parallel conversion hive queries to run. This is the standard gobblin property to control the number of parallel tasks (threads).</p>
         <p>This is set to a default of 50 because each task queries the hive metastore. So this property also limits the number of parallel metastore connections</p>
      </td>
      <td style="word-wrap: break-word">50</td>
   </tr>
   <tr>
      </tr>
      <tr>
         <td style="word-wrap: break-word">
            hive.conversion.avro.flattenedOrc.destination.dbName
         </td>
         <td style="word-wrap: break-word">Name of the ORC database</td>
         <td style="word-wrap: break-word">
            <p>$DB is the Avro database name.</p>
            <p>E.g. If avro database name is tracking, $DB will be resolved at runtime to tracking.</p>
            <ul>
               <li>Setting the value to &quot;$DB_column&quot; will result in a ORC table name of tracking_column</li>
            </ul>
         </td>
      </tr>
      <tr>
         <td style="word-wrap: break-word">
            hive.conversion.avro.flattenedOrc.destination.tableName
         </td>
         <td style="word-wrap: break-word"> Name of the ORC table</td>
         <td style="word-wrap: break-word">
            <p>$TABLE is the Avro table name.</p>
            <p>E.g. If avro table name is LogEvent, $TABLE will be resolved at runtime to LogEvent.</p>
            <ul>
               <li>Setting the value of this property to <span>&quot;$TABLE&quot; will cause the ORC table name to be same as Avro table name.</span></li>
               <li><span>Setting the value to <span>&quot;$TABLE_orc&quot; will result in a ORC table name of LogEvent_orc</span></span></li>
            </ul>
         </td>
      </tr>
      <tr>
         <td style="word-wrap: break-word">
            hive.conversion.avro.flattenedOrc.destination.dataPath
         </td>
         <td style="word-wrap: break-word">Location on HDFS where ORC data is published</td>
         <td style="word-wrap: break-word">/events_orc/$DB/$TABLE</td>
      </tr>
      <tr>
         <td style="word-wrap: break-word">
            hive.conversion.avro.flattenedOrc.evolution.enabled
         </td>
         <td style="word-wrap: break-word">Decides if schema evolution is enabled</td>
         <td style="word-wrap: break-word">true/false</td>
      </tr>
      <tr>
         <td style="word-wrap: break-word">
            hive.conversion.avro.flattenedOrc.hiveRuntime.*
         </td>
         <td style="word-wrap: break-word">
            <p>Additional hive properties to be set while executing the conversion DDL.</p>
            <p>Prefix any hive standard properties with this key</p>
         </td>
         <td style="word-wrap: break-word">
            hive.conversion.avro.flattenedOrc.hiveRuntime.mapred.map.tasks=10
         </td>
      </tr>
      <tr>
         <td style="word-wrap: break-word">
            hive.conversion.avro.destinationFormats
         </td>
         <td style="word-wrap: break-word">A comma separated list of destination formats. Currently supports nestedOrc and flattenedOrc</td>
         <td style="word-wrap: break-word">flattenedOrc,nestedOrc</td>
      </tr>
</table>

# Metrics and Events

SLA event is published every time an Avro partition/table is converted to ORC. Each SLA event has the following metadata.

```
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
![Event metadata description](../img/Avro-to-Orc-timeline.jpg)

# Sample Job

```
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