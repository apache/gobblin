Table of Contents
--------------------

[TOC]

# Getting Started

This section helps you set up a quick-start job for ingesting Kafka topics on a single machine. We provide quick start examples in both standalone and MapReduce mode.

## Standalone

* Setup a single node Kafka broker by following the [Kafka quick start guide](http://kafka.apache.org/documentation.html#quickstart). Suppose your broker URI is `localhost:9092`, and you've created a topic "test" with two events "This is a message" and "This is a another message".

* The remaining steps are the same as the [Wikipedia example](../Getting-Started), except using the following job config properties:

```
job.name=GobblinKafkaQuickStart
job.group=GobblinKafka
job.description=Gobblin quick start job for Kafka
job.lock.enabled=false

kafka.brokers=localhost:9092

source.class=gobblin.source.extractor.extract.kafka.KafkaSimpleSource
extract.namespace=gobblin.extract.kafka

writer.builder.class=gobblin.writer.SimpleDataWriterBuilder
writer.file.path.type=tablename
writer.destination.type=HDFS
writer.output.format=txt

data.publisher.type=gobblin.publisher.BaseDataPublisher

mr.job.max.mappers=1

metrics.reporting.file.enabled=true
metrics.log.dir=${env:GOBBLIN_WORK_DIR}/metrics
metrics.reporting.file.suffix=txt

bootstrap.with.offset=earliest
```

After the job finishes, the following messages should be in the job log:

```
INFO Pulling topic test
INFO Pulling partition test:0 from offset 0 to 2, range=2
INFO Finished pulling partition test:0
INFO Finished pulling topic test
INFO Extracted 2 data records
INFO Actual high watermark for partition test:0=2, expected=2
INFO Task <task_id> completed in 31212ms with state SUCCESSFUL
```

The output file will be in `GOBBLIN_WORK_DIR/job-output/test`, with the two messages you've just created in the Kafka broker. `GOBBLIN_WORK_DIR/metrics` will contain metrics collected from this run.

## MapReduce

* Setup a single node Kafka broker same as in standalone mode.
* Setup a single node Hadoop cluster by following the steps in [Hadoop: Setting up a Single Node Cluster](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html). Suppose your HDFS URI is `hdfs://localhost:9000`.
* Create a job config file with the following properties:

```
job.name=GobblinKafkaQuickStart
job.group=GobblinKafka
job.description=Gobblin quick start job for Kafka
job.lock.enabled=false

kafka.brokers=localhost:9092

source.class=gobblin.source.extractor.extract.kafka.KafkaSimpleSource
extract.namespace=gobblin.extract.kafka

writer.builder.class=gobblin.writer.SimpleDataWriterBuilder
writer.file.path.type=tablename
writer.destination.type=HDFS
writer.output.format=txt

data.publisher.type=gobblin.publisher.BaseDataPublisher

mr.job.max.mappers=1

metrics.reporting.file.enabled=true
metrics.log.dir=/gobblin-kafka/metrics
metrics.reporting.file.suffix=txt

bootstrap.with.offset=earliest

fs.uri=hdfs://localhost:9000
writer.fs.uri=hdfs://localhost:9000
state.store.fs.uri=hdfs://localhost:9000

mr.job.root.dir=/gobblin-kafka/working
state.store.dir=/gobblin-kafka/state-store
task.data.root.dir=/jobs/kafkaetl/gobblin/gobblin-kafka/task-data
data.publisher.final.dir=/gobblintest/job-output
```

* Run `gobblin-mapreduce.sh`:

`gobblin-mapreduce.sh --conf <path-to-job-config-file>`

After the job finishes, the job output file will be in `/gobblintest/job-output/test` in HDFS, and the metrics will be in `/gobblin-kafka/metrics`.


# Job Constructs

## Source and Extractor

Gobblin provides two abstract classes, [`KafkaSource`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/KafkaSource.java) and [`KafkaExtractor`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/KafkaExtractor.java). `KafkaSource` creates a workunit for each Kafka topic partition to be pulled, then merges and groups the workunits based on the desired number of workunits specified by property `mr.job.max.mappers` (this property is used in both standalone and MR mode). More details about how workunits are merged and grouped is available [here](#merging-and-grouping-workunits-in-kafkasource). `KafkaExtractor` extracts the partitions assigned to a workunit, based on the specified low watermark and high watermark.

To use them in a Kafka-HDFS ingestion job, one should subclass `KafkaExtractor` and implement method `decodeRecord(MessageAndOffset)`, which takes a `MessageAndOffset` object pulled from the Kafka broker and decodes it into a desired object. One should also subclass `KafkaSource` and implement `getExtractor(WorkUnitState)` which should return an instance of the Extractor class.

As examples, take a look at [`KafkaSimpleSource`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/KafkaSimpleSource.java), [`KafkaSimpleExtractor`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/KafkaSimpleExtractor.java), and [`KafkaAvroExtractor`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/KafkaExtractor.java).

`KafkaSimpleExtractor` simply returns the payload of the `MessageAndOffset` object as a byte array. A job that uses `KafkaSimpleExtractor` may use a `Converter` to convert the byte array to whatever format desired. For example, if the desired output format is JSON, one may implement an `ByteArrayToJsonConverter` to convert the byte array to JSON. Alternatively one may implement a `KafkaJsonExtractor`, which extends `KafkaExtractor` and convert the `MessageAndOffset` object into a JSON object in the `decodeRecord` method. Both approaches should work equally well. `KafkaAvroExtractor` decodes the payload of the `MessageAndOffset` object into an Avro [`GenericRecord`](http://avro.apache.org/docs/current/api/java/index.html?org/apache/avro/generic/GenericRecord.html) object.

## Writer and Publisher

Any desired writer and publisher can be used, e.g., one may use the [`AvroHdfsDataWriter`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/writer/AvroHdfsDataWriter.java) and the [`BaseDataPublisher`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/publisher/BaseDataPublisher.java), similar as the [Wikipedia example job](../Getting-Started). If plain text output file is desired, one may use [`SimpleDataWriter`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/writer/SimpleDataWriter.java).

# Job Config Properties

These are some of the job config properties used by `KafkaSource` and `KafkaExtractor`.

| Property Name | Semantics     |
| ------------- |-------------| 
| `topic.whitelist` (regex)      | Kafka topics to be pulled. Default value = .* | 
| `topic.blacklist` (regex)     | Kafka topics not to be pulled. Default value = empty | 
| `kafka.brokers` | Comma separated Kafka brokers to ingest data from.      |  
| `mr.job.max.mappers` | Number of tasks to launch. In MR mode, this will be the number of mappers launched. If the number of topic partitions to be pulled is larger than the number of tasks, `KafkaSource` will assign partitions to tasks in a balanced manner.      |  
| `bootstrap.with.offset` | For new topics / partitions, this property controls whether they start at the earliest offset or the latest offset. Possible values: earliest, latest, skip. Default: latest      |
| `reset.on.offset.out.of.range` | This property controls what to do if a partition's previously persisted offset is out of the range of the currently available offsets. Possible values: earliest (always move to earliest available offset), latest (always move to latest available offset), nearest (move to earliest if the previously persisted offset is smaller than the earliest offset, otherwise move to latest), skip (skip this partition). Default: nearest |
| `topics.move.to.latest.offset` (no regex) | Topics in this list will always start from the latest offset (i.e., no records will be pulled). To move all topics to the latest offset, use "all". This property should rarely, if ever, be used.

It is also possible to set a time limit for each task. For example, to set the time limit to 15 minutes, set the following properties:

```
extract.limit.enabled=true
extract.limit.type=time #(other possible values: rate, count, pool)
extract.limit.time.limit=15
extract.limit.time.limit.timeunit=minutes 
```
# Metrics and Events

## Task Level Metrics

Task level metrics can be created in `Extractor`, `Converter` and `Writer` by extending [`InstrumentedExtractor`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/instrumented/extractor/InstrumentedExtractor.java), [`InstrumentedConverter`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/instrumented/converter/InstrumentedConverter.java) and [`InstrumentedDataWriter`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/instrumented/writer/InstrumentedDataWriter.java).

For example, `KafkaExtractor` extends `InstrumentedExtractor`. So you can do the following in subclasses of `KafkaExtractor`:

```
Counter decodingErrorCounter = this.getMetricContext().counter("num.of.decoding.errors");
decodingErrorCounter.inc();
```

Besides Counter, Meter and Histogram are also supported.

## Task Level Events

Task level events can be submitted by creating an [`EventSubmitter`](https://github.com/linkedin/gobblin/blob/master/gobblin-metrics/src/main/java/gobblin/metrics/event/EventSubmitter.java) instance and using `EventSubmitter.submit()` or `EventSubmitter.getTimingEvent()`.

## Job Level Metrics

To create job level metrics, one may extend [`AbstractJobLauncher`](https://github.com/linkedin/gobblin/blob/master/gobblin-runtime/src/main/java/gobblin/runtime/AbstractJobLauncher.java) and create metrics there. For example:

```
Optional<JobMetrics> jobMetrics = this.jobContext.getJobMetricsOptional();
if (!jobMetrics.isPresent()) {
  LOG.warn("job metrics is absent");
  return;
}
Counter recordsWrittenCounter = jobMetrics.get().getCounter("job.records.written");
recordsWrittenCounter.inc(value);
```

Job level metrics are often aggregations of task level metrics, such as the `job.records.written` counter above. Since `AbstractJobLauncher` doesn't have access to task-level metrics, one should set these counters in `TaskState`s, and override `AbstractJobLauncher.postProcessTaskStates()` to aggregate them. For example, in `AvroHdfsTimePartitionedWriter.close()`, property `writer.records.written` is set for the `TaskState`. 

## Job Level Events

Job level events can be created by extending `AbstractJobLauncher` and use `this.eventSubmitter.submit()` or `this.eventSubmitter.getTimingEvent()`.

For more details about metrics, events and reporting them, please see Gobblin Metrics section.

# Grouping Workunits

For each topic partition that should be ingested, `KafkaSource` first retrieves the last offset pulled by the previous run, which should be the first offset of the current run. It also retrieves the earliest and latest offsets currently available from the Kafka cluster and verifies that the first offset is between the earliest and the latest offsets. The latest offset is the last offset to be pulled by the current workunit. Since new records may be constantly published to Kafka and old records are deleted based on retention policies, the earliest and latest offsets of a partition may change constantly.

For each partition, after the first and last offsets are determined, a workunit is created. If the number of Kafka partitions exceeds the desired number of workunits specified by property `mr.job.max.mappers`, `KafkaSource` will merge and group them into `n` [`MultiWorkUnit`](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/workunit/MultiWorkUnit.java)s where `n=mr.job.max.mappers`. This is done using [`KafkaWorkUnitPacker`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/workunit/packer/KafkaWorkUnitPacker.java), which has two implementations: [`KafkaSingleLevelWorkUnitPacker`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/workunit/packer/KafkaSingleLevelWorkUnitPacker.java) and [`KafkaBiLevelWorkUnitPacker`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/workunit/packer/KafkaBiLevelWorkUnitPacker.java). The packer packs workunits based on the estimated size of each workunit, which is obtained from [`KafkaWorkUnitSizeEstimator`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/workunit/packer/KafkaWorkUnitSizeEstimator.java), which also has two implementations, [`KafkaAvgRecordSizeBasedWorkUnitSizeEstimator`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/workunit/packer/KafkaAvgRecordSizeBasedWorkUnitSizeEstimator.java) and [`KafkaAvgRecordTimeBasedWorkUnitSizeEstimator`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/kafka/workunit/packer/KafkaAvgRecordTimeBasedWorkUnitSizeEstimator.java).

## Single-Level Packing

The single-level packer uses a worst-fit-decreasing approach for assigning workunits to mappers: each workunit goes to the mapper that currently has the lightest load. This approach balances the mappers well. However, multiple partitions of the same topic are usually assigned to different mappers. This may cause two issues: (1) many small output files: if multiple partitions of a topic are assigned to different mappers, they cannot share output files. (2) task overhead: when multiple partitions of a topic are assigned to different mappers, a task is created for each partition, which may lead to a large number of tasks and large overhead.

## Bi-Level Packing

The bi-level packer packs workunits in two steps.

In the first step, all workunits are grouped into approximately `3n` groups, each of which contains partitions of the same topic. The max group size is set as

`maxGroupSize = totalWorkunitSize/3n`

The best-fit-decreasing algorithm is run on all partitions of each topic. If an individual workunit’s size exceeds `maxGroupSize`, it is put in a separate group. For each group, a new workunit is created which will be responsible for extracting all partitions in the group.

The reason behind `3n` is that if this number is too small (i.e., too close to `n`), it is difficult for the second level to pack these groups into n balanced multiworkunits; if this number is too big, `avgGroupSize` will be small which doesn’t help grouping partitions of the same topic together. `3n` is a number that is empirically selected.

The second step uses the same worst-fit-decreasing method as the first-level packer.

This approach reduces the number of small files and the number of tasks, but it may have more mapper skew for two reasons: (1) in the worst-fit-decreasing approach, the less number of items to be packed, the more skew there will be; (2) when multiple partitions of a topic are assigned to the same mapper, if we underestimate the size of this topic, this mapper may take a much longer time than other mappers and the entire MR job has to wait for this mapper. This, however, can be mitigated by setting a time limit for each task, as explained above.

## Average Record Size-Based Workunit Size Estimator

This size estimator uses the average record size of each partition to estimate the sizes of workunits. When using this size estimator, each job run will record the average record size of each partition it pulled. In the next run, for each partition the average record size pulled in the previous run is considered the average record size
to be pulled in this run.

If a partition was not pulled in a run, a default value of 1024 will be used in the next run.

## Average Record Time-Based Workunit Size Estimator

This size estimator uses the average time to pull a record in each run to estimate the sizes of the workunits in the next run.

When using this size estimator, each job run will record the average time per record of each partition. In the next run, the estimated average time per record for each topic is the geometric mean of the avg time per record of all partitions. For example if a topic has two partitions whose average time per record in the previous run are 2 and 8, the next run will use 4 as the estimated average time per record.

If a topic is not pulled in a run, its estimated average time per record is the geometric mean of the estimated average time per record of all topics that are pulled in this run. If no topic was pulled in this run, a default value of 1.0 is used.

The time-based estimator is more accurate than the size-based estimator when the time to pull a record is not proportional to the size of the record. However, the time-based estimator may lose accuracy when there are fluctuations in the Hadoop cluster which causes the average time for a partition to vary between different runs.

# Topic-Specific Configuration

`kafka.topic.specific.state` is a configuration key that allows a user to specify config parameters on a topic specific level. The value of this config should be a JSON Array. Each entry should be a json string and should contain a primitive value that identifies the topic name. All configs in each topic entry will be added to the WorkUnit for that topic.

An example value could be:

```
[
  {
    "dataset": "myTopic1",
    "writer.partition.columns": "header.memberId"
  },
  {
    "dataset": "myTopic2",
    "writer.partition.columns": "auditHeader.time"
  }
]
```

The `dataset` field also allows regular expressions. For example, one can specify key, value `"dataset" : "myTopic.\*"`. In this case all topics whose name matches the pattern `myTopic.*` will have all the specified config properties added to their WorkUnit. If more than one topic matches multiple `dataset`s then the properties from all the JSON objects will be added to their WorkUnit.

# Kafka `Deserializer` Integration

Gobblin integrates with Kafka's [Deserializer](https://kafka.apache.org/0100/javadoc/org/apache/kafka/common/serialization/Deserializer.html) API. Kafka's `Deserializer` Interface offers a generic interface for Kafka Clients to deserialize data from Kafka into Java Objects. Since Kafka Messages return byte array, the `Deserializer` class offers a convienient way of transforming those byte array's to Java Objects.

Kafka's Client Library already has a few useful `Deserializer`s such as the the [StringDeserializer](https://kafka.apache.org/0100/javadoc/org/apache/kafka/common/serialization/StringDeserializer.html) and the [ByteBufferDeserializer](https://kafka.apache.org/0100/javadoc/org/apache/kafka/common/serialization/ByteBufferDeserializer.html).

Gobblin can integrate with any of these `Deserializer`s, that is any class that implements the `Deserializer` interface can be used to convert Kafka message to Java Objects. This is done in the `KafkaDeserializerSource` and the `KafkaDeserializerExtractor` classes.

The type of `Deserializer` to be used in `KafkaDeserializerExtractor` can be specified by the property `kafka.deserializer.type`. This property can either be set to any of the pre-defined `Deserializer`s such as `CONFLUENT_AVRO`, `CONFLUENT_JSON`, `GSON`, `BYTE_ARRAY`, and `STRING` (see the section on [Confluent Integration](#confluent-integration) and [KafkaGsonDeserializer](#kafkagsondeserializer) for more details). The value of this property can point to the full-qualified path of a `Deserializer` implementation. If the value is set a class name, then a `kafka.schema.registry.class` must also be provided so that the `Extractor` knows how to retrieve the schema for the topic.

## Gobblin `Deserializer` Implementations

### KafkaGsonDeserializer

The `KafkaGsonDeserializer` is an implementation of the `Deserializer` class that converts `byte[]` to [JSONObject](https://google.github.io/gson/apidocs/com/google/gson/JsonObject.html)s. It uses [GSON](https://github.com/google/gson) to do this.

This class is useful for converting Kafka data to JSON Objects.

Using this class simply requires setting `kafka.deserializer.type` to `GSON`.

## Comparison with `KafkaSimpleSource`

Gobblin's `KafkaSimpleSource` and `KafkaSimpleExtractor` are very useful when data just needs to be read from Kafka and written to a text file. However, it does not provide good support for writing to more complex data file formats such as [Avro](https://avro.apache.org/) or [ORC](https://orc.apache.org/). It also doesn't provide good support for record level manipulations such as Gobblin `Converter`s and it lacks good support for use with Gobblin's `WriterPartitioner`. The reason is that `KafkaSimpleExtractor` simply returns a `byte[]`, which is just a black-box of data. It is much easier to maniuplate the record if it is converted to a Java Object. This is where Gobblin's `KafkaDeserializerExtractor` becomes useful.

# Confluent Integration

[Confluent](http://www.confluent.io/) provides a standardized distribution of [Apache Kafka](http://kafka.apache.org/), along with other useful tools for working with Kafka. One useful tool that Confluent provides is a generic [Schema Registry](http://www.confluent.io/blog/schema-registry-kafka-stream-processing-yes-virginia-you-really-need-one).

Gobblin has integration with [Confluent's Schema Registry Library](https://github.com/confluentinc/schema-registry) which provides a service to register and get [Avro Schemas](https://avro.apache.org/docs/1.8.0/spec.html) and provides a generic [Avro Deserializer](https://github.com/confluentinc/schema-registry/blob/master/avro-serializer/src/main/java/io/confluent/kafka/serializers/KafkaAvroDeserializer.java) and [JSON Deserializer](https://github.com/confluentinc/schema-registry/blob/master/json-serializer/src/main/java/io/confluent/kafka/serializers/KafkaJsonDeserializer.java).

## Confluent Schema Registry

Gobblin integrates with Confluent's [SchemaRegistryClient](https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClient.java) class in order to register and get Avro Schema's from the Confluent [SchemaRegistry](https://github.com/confluentinc/schema-registry/blob/master/core/src/main/java/io/confluent/kafka/schemaregistry/storage/SchemaRegistry.java). This is implemented in the `ConfluentKafkaSchemaRegistry` class, which extends Gobblin's `KafkaSchemaRegistry` class. The `ConfluentKafkaSchemaRegistry` can be used by setting `kafka.schema.registry.class` to `gobblin.source.extractor.extract.kafka.ConfluentKafkaSchemaRegistry`.

## Confluent Deserializers

Confluent's Schema Registry Library also provides a few useful `Deserializer` implementations:

* [KafkaAvroDeserializer](https://github.com/confluentinc/schema-registry/blob/master/avro-serializer/src/main/java/io/confluent/kafka/serializers/KafkaAvroDeserializer.java) 
* [KafkaJsonDeserializer](https://github.com/confluentinc/schema-registry/blob/master/json-serializer/src/main/java/io/confluent/kafka/serializers/KafkaJsonDeserializer.java)

With regards to Gobblin, these classes are useful if Confluent's [KafkaAvroSerializer](https://github.com/confluentinc/schema-registry/blob/master/avro-serializer/src/main/java/io/confluent/kafka/serializers/KafkaAvroSerializer.java) or [KafkaJsonSerializer](https://github.com/confluentinc/schema-registry/blob/master/json-serializer/src/main/java/io/confluent/kafka/serializers/KafkaJsonSerializer.java) is used to write data to Kafka.

The [Serializer](https://kafka.apache.org/0100/javadoc/org/apache/kafka/common/serialization/Serializer.html) class is a Kafka interface that is the converse of the `Deserializer` class. The `Serializer` provides a generic way of taking Java Objects and converting them to `byte[]` that are written to Kafka by a `KafkaProducer`.

### KafkaAvroDeserializer

Documentation for the `KafkaAvroDeserializer` can be found [here](http://docs.confluent.io/2.0.1/schema-registry/docs/serializer-formatter.html#serializer).

If data is written to a Kafka cluster using Confluent's `KafkaAvroSerializer`, then the `KafkaAvroDeserializer` should be used in Gobblin. Setting this up simply requires a setting the config key `kafka.deserializer.type` to `CONFLUENT_AVRO` (see the section on [Kafka Deserializer Integration](#kafka-deserializer-integration) for more information).

### KafkaJsonDeserializer

The `KafkaJsonDeserializer` class uses [Jackson's Object Mapper](https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/ObjectMapper.html) to convert `byte[]` to Java Objects. In order to `KafkaJsonDeserializer` to know which class the `byte[]` array should be converted to, the config property `json.value.type` needs to be set to the fully-qualified class name of the Java Object that the `Deserializer` should return. For more information about how the Jackson works, check out the docs [here](https://github.com/FasterXML/jackson-databind).

Using the `KafkaJsonDeserializer` simply requires setting the config key `kafka.deserializer.type` to `CONFLUENT_JSON` (see the section on [Kafka Deserializer Integration](#kafka-deserializer-integration) for more information).
