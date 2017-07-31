Table of Contents
--------------------

[TOC]

Compaction can be used to post-process files pulled by Gobblin with certain semantics. Deduplication is one of the common reasons to do compaction, e.g., you may want to

* deduplicate on all fields of the records.
* deduplicate on key fields of the records, keep the one with the latest timestamp for records with the same key.

This is because duplicates can be generated for multiple reasons including both intended and unintended:

* For ingestion from data sources with mutable records (e.g., relational databases), instead of ingesting a full snapshot of a table every time, one may wish to ingest only the records that were changed since the previous run (i.e., delta records), and merge these delta records with previously generated snapshots in a compaction. In this case, for records with the same primary key, the one with the latest timestamp should be kept.
* The data source you ingest from may have duplicate records, e.g., if you have a hierarchy of Kafka clusters where topics are replicated among the Kafka clusters, duplicate records may be generated during the replication. In some data sources duplicate records may also be produced by the data producer.
* In rare circumstances, Gobblin may pull the same data twice, thus creating duplicate records. This may happen if Gobblin publishes the data successfully, but for some reason fails to persist the checkpoints (watermarks) into the state store.

Gobblin provides two compactors out-of-the-box, a MapReduce compactor and a Hive compactor.

# MapReduce Compactor

The MapReduce compactor can be used to deduplicate on all or certain fields of the records. For duplicate records, one of them will be preserved; there is no guarantee which one will be preserved.

A use case of MapReduce Compactor is for Kafka records deduplication. We will use the following example use case to explain the MapReduce Compactor.

## Example Use Case

Suppose we ingest data from a Kafka broker, and we would like to publish the data by hour and by day, both of which are deduplicated:

- Data in the Kafka broker is first ingested into an `hourly_staging` folder, e.g., `/data/kafka_topics/PageViewEvent/hourly_staging/2015/10/29/08...`
- A compaction with deduplication runs hourly, consumes data in `hourly_staging` and publish data into `hourly`, e.g., `/data/kafka_topics/PageViewEvent/hourly/2015/10/29/08...`
- A non-deduping compaction runs daily, consumes data in `hourly` and publish data into `daily`, e.g., `/data/kafka_topics/PageViewEvent/daily/2015/10/29...`

## Basic Usage

`MRCompactor.compact()` is the entry point for MapReduce-based compaction. The compaction unit is [`Dataset`](https://github.com/linkedin/gobblin/blob/master/gobblin-compaction/src/main/java/gobblin/compaction/dataset/Dataset.java). `MRCompactor` uses a [`DatasetsFinder`](https://github.com/linkedin/gobblin/blob/master/gobblin-compaction/src/main/java/gobblin/compaction/dataset/DatasetsFinder.java) to find all datasets eligible for compaction. Implementations of `DatasetsFinder` include [`SimpleDatasetsFinder`](https://github.com/linkedin/gobblin/blob/master/gobblin-compaction/src/main/java/gobblin/compaction/dataset/SimpleDatasetsFinder.java) and [`TimeBasedSubDirDatasetsFinder`](https://github.com/linkedin/gobblin/blob/master/gobblin-compaction/src/main/java/gobblin/compaction/dataset/TimeBasedSubDirDatasetsFinder.java).

In the above example use case, for hourly compaction, each dataset contains an hour's data in the `hourly_staging` folder, e.g., `/data/kafka_topics/PageViewEvent/hourly_staging/2015/10/29/08`; for daily compaction, each dataset contains 24 hourly folder of a day, e.g., `/data/kafka_topics/PageViewEvent/hourly/2015/10/29`. In hourly compaction, you may use the following config properties:

```
compaction.datasets.finder=org.apache.gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder
compaction.input.dir=/data/kafka_topics
compaction.dest.dir=/data/kafka_topics
compaction.input.subdir=hourly_staging
compaction.dest.subdir=hourly
compaction.folder.pattern=YYYY/MM/dd
compaction.timebased.max.time.ago=3h
compaction.timebased.min.time.ago=1h
compaction.jobprops.creator.class=org.apache.gobblin.compaction.mapreduce.MRCompactorTimeBasedJobPropCreator
compaction.job.runner.class=org.apache.gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner (if your data is Avro)
```

If your data format is not Avro, you can implement a different job runner class for deduplicating your data format. 

## Non-deduping Compaction via Map-only Jobs

There are two types of Non-deduping compaction.

- **Type 1**: deduplication is not needed, for example you simply want to consolidate files in 24 hourly folders into a single daily folder.
- **Type 2**: deduplication is needed, i.e., the published data should not contain duplicates, but the input data are already deduplicated. The daily compaction in the above example use case is of this type.

Property `compaction.input.deduplicated` specifies whether the input data are deduplicated (default is false), and property `compaction.output.deduplicated` specifies whether the output data should be deduplicated (default is true). For type 1 deduplication, set both to false. For type 2 deduplication, set both to true.

The reason these two types of compaction need to be separated is because of late data handling, which we will explain next.

## Handling Late Records

Late records are records that arrived at a folder after compaction on this folder has started. We explain how Gobblin handles late records using the following example.

In this use case, both hourly compaction and daily compaction need a mechanism to handle late records. For hourly compaction, late records are records that arrived at an `hourly_staging` folder after the hourly compaction of that folder has started. It is similar for daily compaction.

**Compaction with Deduplication**

For a compaction with deduplication (i.e., hourly compaction in the above use case), there are two options to deal with late data:

- **Option 1**: if there are late data, re-do the compaction. For example, you may run the hourly compaction multiple times per hour. The first run will do the normal compaction, and in each subsequent run, if it detects late data in a folder, it will re-do compaction for that folder.

To do so, set `compaction.job.overwrite.output.dir=true` and `compaction.recompact.from.input.for.late.data=true`.

Please note the following when you use this option: (1) this means that your already-published data will be re-published if late data are detected; (2) this is potentially dangerous if your input folders have short retention periods. For example, suppose `hourly_staging` folders have a 2-day retention period, i.e., folder `/data/kafka_topics/PageViewEvent/hourly_staging/2015/10/29` will be deleted on 2015/10/31. If, after 2015/10/31, new data arrived at this folder and you re-compact this folder and publish the data to `hourly`, all original data will be gone. To avoid this problem you may set `compaction.timebased.max.time.ago=2d` so that compaction will not be performed on a folder more than 2 days ago. However, this means that if a late record is late for more than 2 days, it will never be published into `hourly`.

- **Option 2**: (this is the default option) if there are late data, copy the late data into a `[output_subdir]/_late` folder, e.g., for hourly compaction, late data in `hourly_staging` will be copied to `hourly_late` folders, e.g., `/data/kafka_topics/PageViewEvent/hourly_late/2015/10/29...`. 

If re-compaction is not necessary, this is all you need to do. If re-compaction is needed, you may schedule or manually invoke a re-compaction job which will re-compact by consuming data in both `hourly` and `hourly_late`. For this job, you need to set `compaction.job.overwrite.output.dir=true` and `compaction.recompact.from.dest.paths=true`.

Note that this re-compaction is different from the re-compaction in Option 1: this re-compaction consumes data in output folders (i.e., `hourly`) whereas the re-compaction in Option 1 consumes data in input folders (i.e., `hourly_staging`).

**Compaction without Deduplication**

For a compaction without deduplication, if it is type 2, the same two options above apply. If it is type 1, late data will simply be copied to the output folder.

**How to Determine if a Data File is Late**

Every time a compaction finishes (except the case below), Gobblin will create a file named `_COMPACTION_COMPLETE` in the compaction output folder. This file contains the timestamp of when the compaction job starts. All files in the input folder with earlier modification timestamps have been compacted. Next time the compaction runs, files in the input folder with later timestamps are considered late data.

The `_COMPACTION_COMPLETE` file will be only be created if it is a regular compaction that consumes input data (including compaction jobs that just copy late data to the output folder or the `[output_subdir]/_late` folder without launching an MR job). It will not be created if it is a re-compaction that consumes output data. This is because whether a file in the input folder is a late file depends on whether it has been compacted or moved into the output folder, which is not affected by a re-compaction that consumes output data.

One way of reducing the chance of seeing late records is to verify data completeness before running compaction, which will be explained next.

## Verifying Data Completeness Before Compaction

Besides aborting the compaction job for a dataset if new data in the input folder is found, another way to reduce the chance of seeing late events is to verify the completeness of input data before running compaction. To do so, set `compaction.completeness.verification.enabled=true`, extend `DataCompletenessVerifier.AbstractRunner` and put in your verification logic, and pass it via `compaction.completeness.verification.class`.

When data completeness verification is enabled, `MRCompactor` will verify data completeness for the input datasets, and meanwhile speculatively start the compaction MR jobs. When the compaction MR job for a dataset finishes, if the completeness of the dataset is verified, its compacted data will be published, otherwise it is discarded, and the compaction MR job for this dataset will be launched again with a reduced priority.

It is possible to control which topics should or should not be verified via `compaction.completeness.verification.whitelist` and `compaction.completeness.verification.blacklist`. It is also possible to set a timeout for data completeness verification via `compaction.completeness.verification.timeout.minutes`. A dataset whose completeness verification timed out can be configured to be either compacted anyway or not compacted.

# Hive Compactor

The Hive compactor can be used to merge a snapshot with one or multiple deltas. It assumes the snapshot and the deltas meet the following requirements:

1. Snapshot and all deltas are in Avro format.
1. Snapshot and all deltas have the same primary key attributes (they do not need to have the same schema).
2. Snapshot is pulled earlier than all deltas. Therefore if a key appears in both snapshot and deltas, the one in the snapshot should be discarded.
3. The deltas are pulled one after another, and ordered in ascending order of pull time. If a key appears in both the ith delta and the jth delta (i < j), the one in the jth delta survives.

The merged data will be written to the HDFS directory specified in `output.datalocation`, as one or more Avro files. The schema of the output data will be the same as the schema of the last delta (which is the last pulled data and thus has the latest schema).

In the near future we also plan to support selecting records by timestamps (rather than which file they appear). This is useful if the snapshot and the deltas are pulled in parallel, where if a key has multiple occurrences we should keep the one with the latest timestamp.

Note that since delta tables don't have information of deleted records, such information is only available the next time the full snapshot is pulled.

## Basic Usage

A Hive Compactor job consists of one global configuration file which refers to one or more job configuration(s).  

### Global Config Properties (example: compaction.properties)

(1) Required:

- _**compaction.config.dir**_

This is the the compaction jobconfig directory. Each file in this directory should be a jobconfig file (described in the next section).

(2) Optional:

- _**hadoop.configfile.***_

Hadoop configuration files that should be loaded
(e.g., hadoop.configfile.coresite.xml=/export/apps/hadoop/latest/etc/hadoop/core-site.xml)

- _**hdfs.uri**_

If property `fs.defaultFS` (or `fs.default.name`) is specified in the hadoop config file, then this property is not needed. However, if it is specified, it will override `fs.defaultFS` (or `fs.default.name`).

If `fs.defaultFS` or `fs.default.name` is not specified in the hadoop config file, and this property is also not specified, then the default value "hdfs://localhost:9000" will be used.

- _**hiveserver.version**_ (default: 2)

Either 1 or 2.

- _**hiveserver.connection.string**_

- _**hiveserver.url**_

- _**hiveserver.user**_ (default: "")

- _**hiveserver.password**_ (default: "")

If `hiveserver.connection.string` is specified, it will be used to connect to hiveserver.

If `hiveserver.connection.string` is not specified but `hiveserver.url` is specified, then it uses (`hiveserver.url`, `hiveserver.user`, `hiveserver.password`) to connect to hiveserver.

If neither `hiveserver.connection.string` nor `hiveserver.url` is specified, then embedded hiveserver will be used (i.e., `jdbc:hive://` if `hiveserver.version=1`, `jdbc:hive2://` if `hiveserver.version=2`)

- _**hivesite.dir**_

Directory that contains hive-site.xml, if hive-site.xml should be loaded.

- _**hive.***_

Any hive config property. (e.g., `hive.join.cache.size`). If specified, it will override the corresponding property in hive-site.xml.


### Job Config Properties (example: jobconf/task1.conf)

(1) Required:

- _**snapshot.pkey**_

comma separated primary key attributes of the snapshot table

- _**snapshot.datalocation**_

snapshot data directory in HDFS

- _**delta.i.pkey**_ (i = 1, 2...)

the primary key of ith delta table
(the primary key of snapshot and all deltas should be the same)

- _**delta.i.datalocation**_ (i = 1, 2...)

ith delta table's data directory in HDFS

- _**output.datalocation**_

the HDFS data directory for the output
(make sure you have write permission on this directory)

(2) Optional:

- _**snapshot.name**_ (default: randomly generated name)

prefix name of the snapshot table. The table name will be snapshot.name + random suffix

- _**snapshot.schemalocation**_

snapshot table's schema location in HDFS. If not specified, schema will be extracted from the data.

- _**delta.i.name**_ (default: randomly generated name)

prefix name of the ith delta table. The table name will be delta.i.name + random suffix

- _**delta.i.schemalocation**_

ith delta table's schema location in HDFS. If not specified, schema will be extracted from the data.

- _**output.name**_ (default: randomly generated name)

prefix name of the output table. The table name will be output.name + random suffix

- _**hive.db.name**_ (default: default)

the database name to be used. This database should already exist, and you should have write permission on it.

- _**hive.queue.name**_ (default: default)

queue name to be used.

- _**hive.use.mapjoin**_ (default: if not specified in the global config file, then false)

whether map-side join should be turned on. If specified both in this property and in the global config file (hive.*), this property takes precedences. 

- _**hive.mapjoin.smalltable.filesize**_ (default: if not specified in the global config file, then use Hive's default value)

if hive.use.mapjoin = true, mapjoin will be used if the small table size is smaller than hive.mapjoin.smalltable.filesize (in bytes).
If specified both in this property and in the global config file (hive.*), this property takes precedences. 

- _**hive.tmpschema.dir**_ (default: the parent dir of the data location dir where the data is used to extract the schema)

If we need to extract schema from data, this dir is for the extracted schema.
Note that if you do not have write permission on the default dir, you must specify this property as a dir where you do have write permission.

- _**snapshot.copydata**_ (default: false)

Set to true if you don't want to (or are unable to) create external table on snapshot.datalocation. A copy of the snapshot data will be created in `hive.tmpdata.dir`, and will be removed after the compaction.

This property should be set to true if either of the following two situations applies:

(i) You don't have write permission to `snapshot.datalocation`. If so, once you create an external table on `snapshot.datalocation`, you may not be able to drop it. This is a Hive bug and for more information, see [this page](https://issues.apache.org/jira/browse/HIVE-9020), which includes a Hive patch for the bug.

(ii) You want to use a certain subset of files in `snapshot.datalocation` (e.g., `snapshot.datalocation` contains both .csv and .avro files but you only want to use .avro files)

- _**delta.i.copydata**_ (i = 1, 2...) (default: false)

Similar as `snapshot.copydata`

- _**hive.tmpdata.dir**_ (default: "/")

If `snapshot.copydata` = true or `delta.i.copydata` = true, the data will be copied to this dir. You should have write permission to this dir.

- _**snapshot.dataformat.extension.name**_ (default: "")

If `snapshot.copydata` = true, then only those data files whose extension is `snapshot.dataformat` will be moved to `hive.tmpdata.dir`.

- _**delta.i.dataformat.extension.name**_ (default: "")

Similar as `snapshot.dataformat.extension.name`. 

- _**mapreduce.job.num.reducers**_

Number of reducers for the job.

- _**timing.file**_ (default: time.txt)

A file where the running time of each compaction job is printed.


# Running a Compaction Job

Both the MapReduce and Hive-based compaction configurations can be executed with `bin/gobblin-compaction.sh` .

The usage is as follows:
```
gobblin-compaction.sh [OPTION] --type <compaction type: hive or mr> --conf <compaction configuration file>
Where OPTION can be:
  --projectversion <version>    Gobblin version to be used. If set, overrides the distribution build version
  --logdir <log dir>            Gobblin's log directory: if not set, taken from ${GOBBLIN_LOG_DIR} if present. 
  --help                        Display this help and exit
```

Example:
```
cd gobblin-dist
bin/gobblin-compaction.sh --type hive --conf compaction.properties
```

The log4j configuration is read from `conf/log4j-compaction.xml`.
Please note that in case of a Hive-compaction for drop table queries (`DROP TABLE IF EXISTS <tablename>`), the Hive JDBC client will throw `NoSuchObjectException` if the table doesn't exist. This is normal and such exceptions should be ignored.
