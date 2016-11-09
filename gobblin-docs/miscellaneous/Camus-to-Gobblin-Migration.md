Table of Contents
-----------------

[TOC]

This page is a guide for [Camus](https://github.com/linkedin/camus) → Gobblin migration, intended for users and organizations currently using Camus. Camus is LinkedIn's previous-generation Kafka-HDFS pipeline.

It is recommended that one read [Kafka-HDFS Ingestion](../case-studies/Kafka-HDFS-Ingestion) before reading this page. This page focuses on the Kafka-related configuration properties in Gobblin vs Camus.

## Advantages of Migrating to Gobblin

**Operability**: Gobblin is a generic data ingestion pipeline that supports not only Kafka but several other data sources, and new data sources can be easily added. If you have multiple data sources, using a single tool to ingest data from these sources is a lot more pleasant operationally than deploying a separate tool for each source.

**Performance**: The performance of Gobblin in MapReduce mode is comparable to Camus', and faster in some cases (e.g., the average record size of a Kafka topic is not proportional to the average time of pulling a topic) due to a better mapper load balancing algorithm. In the new continuous ingestion mode (currently under development), the performance of Gobblin will further improve.

**Metrics and Monitoring**: Gobblin has a powerful end-to-end metrics collection and reporting module for monitoring purpose, making it much easier to spot problems in time and find the root causes. See the "Gobblin Metrics" section in the wiki and [this post](../metrics/Gobblin-Metrics-next-generation-instrumentation-for-applications) for more details.

**Features**: In addition to the above, there are several other useful features for Kafka-HDFS ingestion in Gobblin that are not available in Camus, e.g., [handling late events in data compaction](../user-guide/Compaction#handling-late-records); dataset retention management; converter and quality checker; all-or-nothing job commit policy, etc. Also, Gobblin is under active development and new features are added frequently.

## Kafka Ingestion Related Job Config Properties

This list contains Kafka-specific properties. For general configuration properties please refer to [Configuration Properties Glossary](../user-guide/Configuration-Properties-Glossary).

### Config properties for pulling Kafka topics

| Gobblin Property   |  Corresponding Camus Property | Default value |
|----------|-------------|:------:|
| topic.whitelist |  kafka.whitelist.topics | .*|
| topic.blacklist |  kafka.blacklist.topics  | a^ |
| mr.job.max.mappers | mapred.map.tasks | 100 |
| kafka.brokers  | kafka.host.url | (required) |
| topics.move.to.latest.offset  | kafka.move.to.last.offset.list | empty |
| bootstrap.with.offset  | none | latest |
| reset.on.offset.out.of.range | none | nearest |

Remarks:

* topic.whitelist and topic.blacklist supports regex.
* topics.move.to.latest.offset: Topics in this list will always start from the latest offset (i.e., no records will be pulled). To move all topics to the latest offset, use "all". This property is useful in Camus for moving a new topic to the latest offset, but in Gobblin it should rarely, if ever, be used, since you can use bootstrap.with.offset to achieve the same purpose more conveniently.
* bootstrap with offset: For new topics / partitions, this property controls whether they start at the earliest offset or the latest offset. Possible values: earliest, latest, skip.
* reset.on.offset.out.of.range: This property controls what to do if a partition's previously persisted offset is out of the range of the currently available offsets. Possible values: earliest (always move to earliest available offset), latest (always move to latest available offset), nearest (move to earliest if the previously persisted offset is smaller than the earliest offset, otherwise move to latest), skip (skip this partition).

### Config properties for compaction

Gobblin compaction is comparable to Camus sweeper, which can deduplicate records in an input folder. Compaction is useful for Kafka-HDFS ingestion for two reasons:

1. Although Gobblin guarantees no loss of data, in rare circumstances where data is published on HDFS but checkpoints failed to be persisted into the state store, it may pull the same records twice.

2. If you have a hierarchy of Kafka clusters where topics are replicated among the Kafka clusters, duplicate records may be generated during replication.

Below are the configuration properties related to compaction. For more information please visit the MapReduce Compaction section in the [Compaction](../user-guide/Compaction) page.

| Gobblin Property   |  Corresponding Camus Property | Default value |
|----------|-------------|:------:|
| compaction.input.dir |  camus.sweeper.source.dir | (required) |
| compaction.dest.dir |  camus.sweeper.dest.dir | (required) |
| compaction.input.subdir |  camus.sweeper.source.dir | hourly |
| compaction.dest.subdir |  camus.sweeper.dest.dir | daily |
| compaction.tmp.dest.dir | camus.sweeper.tmp.dir | /tmp/gobblin-compaction |
| compaction.whitelist |  camus.sweeper.whitelist | .* |
| compaction.blacklist |  camus.sweeper.blacklist | a^ |
| compaction.high.priority.topics | none |a^|
| compaction.normal.priority.topics | none |a^|
| compaction.input.deduplicated | none | false |
| compaction.output.deduplicated | none | true |
| compaction.file.system.uri | none ||
| compaction.timebased.max.time.ago |  none | 3d |
| compaction.timebased.min.time.ago | none | 1d |
| compaction.timebased.folder.pattern | none | YYYY/mm/dd |
| compaction.thread.pool.size | num.threads | 20 |
| compaction.max.num.reducers | max.files | 900 |
| compaction.target.output.file.size | camus.sweeper.target.file.size | 268435456 |
| compaction.mapred.min.split.size | mapred.min.split.size | 268435456 |
| compaction.mapred.max.split.size | mapred.max.split.size | 268435456 |
| compaction.mr.job.timeout.minutes | none | |

Remarks:

* The following properties support regex: compaction.whitelist, compaction.blacklist, compaction.high.priority.topics, compaction.normal.priority.topics
* compaction.input.dir is the parent folder of input topics, e.g., /data/kafka_topics, which contains topic folders such as /data/kafka_topics/Topic1, /data/kafka_topics/Topic2, etc. Note that Camus uses camus.sweeper.source.dir both as the input folder of Camus sweeper (i.e., compaction), and as the output folder for ingesting Kafka topics. In Gobblin, one should use data.publisher.final.dir as the output folder for ingesting Kafka topics.
* compaction.output.dir is the parent folder of output topics, e.g., /data/compacted_kafka_topics.
* compaction.input.subdir is the subdir name of output topics, if exists. For example, if the input topics are partitioned by hour, e.g., /data/kafka_topics/Topic1/hourly/2015/10/06/20, then compaction.input.subdir should be 'hourly'.
* compaction.output.subdir is the subdir name of output topics, if exists. For example, if you want to publish compacted data into day-partitioned folders, e.g., /data/compacted_kafka_topics/Topic1/daily/2015/10/06, then compaction.output.subdir should be 'daily'.
* There are 3 priority levels: high, normal, low. Topics not included in compaction.high.priority.topics or compaction.normal.priority.topics are considered low priority.
* compaction.input.deduplicated and compaction.output.deduplicated controls the behavior of the compaction regarding deduplication. Please see the [Compaction](../user-guide/Compaction) page for more details.
* compaction.timebased.max.time.ago and compaction.timebased.min.time.ago controls the earliest and latest input folders to process, when using `MRCompactorTimeBasedJobPropCreator`. The format is ?m?d?h, e.g., 3m or 2d10h (m = month, not minute). For example, suppose `compaction.timebased.max.time.ago=3d`, `compaction.timebased.min.time.ago=1d` and the current time is 10/07 9am. Folders whose timestamps are before 10/04 9am, or folders whose timestamps are after 10/06 9am will not be processed.
* compaction.timebased.folder.pattern: time pattern in the folder path, when using `MRCompactorTimeBasedJobPropCreator`. This should come after `compaction.input.subdir`, e.g., if the input folder to a compaction job is `/data/compacted_kafka_topics/Topic1/daily/2015/10/06`, this property should be `YYYY/mm/dd`.
* compaction.thread.pool.size: how many compaction MR jobs to run concurrently.
* compaction.max.num.reducers: max number of reducers for each compaction job
* compaction.target.output.file.size: This also controls the number of reducers. The number of reducers will be the smaller of `compaction.max.num.reducers` and `<input data size> / compaction.target.output.file.size`.
* compaction.mapred.min.split.size and compaction.mapred.max.split.size are used to control the number of mappers.

## Deployment and Checkpoint Management

For deploying Gobblin in standalone or MapReduce mode, please see the [Deployment](../user-guide/Gobblin-Deployment) page.

Gobblin and Camus checkpoint management are similar in the sense that they both create checkpoint files in each run, and the next run will load the checkpoint files created by the previous run and start from there. Their difference is that Gobblin creates a single checkpoint file per job run or per dataset per job run, and provides two job commit policies: `full` and `partial`. In `full` mode, data are only commited for the job/dataset if all workunits of the job/dataset succeeded. Otherwise, the checkpoint of all workunits/datasets will be rolled back. Camus writes one checkpoint file per mapper, and only supports the `partial` mode. For Gobblin's state management, please refer to the [Wiki page](../user-guide/State-Management-and-Watermarks) for more information.

## Migrating from Camus to Gobblin in Production

If you are currently running in production, you can use the following steps to migrate to Gobblin:

1. Deploy Gobblin based on the instructions in [Deployment](../user-guide/Gobblin-Deployment) and [Kafka-HDFS Ingestion](../case-studies/Kafka-HDFS-Ingestion), and set the properties mentioned in this page as well as other relevant properties in [Configuration Glossary](../user-guide/Configuration-Properties-Glossary) to the appropriate values.
2. Whitelist the topics in Gobblin ingestion, and schedule Gobblin to run at your desired frequency.
3. Once Gobblin starts running, blacklist these topics in Camus.
4. If compaction is applicable to you, set up the compaction jobs based on instructions in [Kafka-HDFS Ingestion](../case-studies/Kafka-HDFS-Ingestion) and [Compaction](../user-guide/Compaction). Whitelist the topics you want to migrate in Gobblin and blacklist them in Camus.
