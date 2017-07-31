Table of Contents
------------------

[TOC]

# Introduction

The Kafka writer allows users to create pipelines that ingest data from Gobblin sources into Kafka. This also enables Gobblin users to seamlessly transition their pipelines from ingesting directly to HDFS to ingesting into Kafka first, and then ingesting from Kafka to HDFS.

#Pre-requisites

* The following guide assumes that you are somewhat familiar with running Gobblin. If not, you should follow the [Getting Started](Getting-Started) page first, then come back to this guide.

* Before you can use the Kafka writer, you need to set up a Kafka cluster to write to. You can follow any of the guides listed by the Kafka project such as the [Apache Kafka quickstart guide](http://kafka.apache.org/documentation.html#quickstart).

#Steps 

* Edit the [wikipedia-kafka.pull](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia-kafka.pull) example to get started with setting up ingestion into Kafka. This is a very similar pipeline to the [wikipedia.pull](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull) example which pulls pages from 5 titles from Wikipedia to HDFS. The main differences to note are: 
    * The `writer.builder.class` is set to `gobblin.kafka.writer.KafkaDataWriterBuilder`. This is the class that creates a Kafka writer.
    * The `writer.kafka.topic` is set to `WikipediaExample`. This is the topic that the writer will write the records to.
    * The `writer.kafka.producerConfig.bootstrap.servers` is set to `localhost:9092`. This is the address of the kafka broker(s) that the writer must write to.
    * There is no partitioner class specified. This implementation of the Kafka writer does not support partitioning and will use the default Kafka partitioner. 
    * The `data.publisher.type` is set to `gobblin.publisher.NoopPublisher`. This is because Kafka doesn't offer transactional semantics, so it isn't possible to have a separate publish step to finally commit the data. 
    * There is configuration for setting up the Schema Registry and Serializers that you will be using to write the data to Kafka. If you're using the Apache Kafka distribution, this file should work out of the box. 
    * If you're using the Confluent distribution and want to use the Confluent schema registry, comment out the Local Schema Registry section and un-comment the Confluent schema registry section. The result should match the text below for Confluent users.

```
#Confluent Schema Registry and serializers
writer.kafka.producerConfig.value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
writer.kafka.producerConfig.key.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
writer.kafka.producerConfig.schema.registry.url=http://localhost:8081  #Set this to the correct schema-reg url

##Use Local Schema Registry and serializers
#writer.kafka.producerConfig.value.serializer=org.apache.gobblin.kafka.serialize.LiAvroSerializer
#writer.kafka.producerConfig.kafka.schemaRegistry.class=org.apache.gobblin.kafka.schemareg.ConfigDrivenMd5SchemaRegistry
#writer.kafka.producerConfig.schemaRegistry.schema.name=WikipediaExample
#writer.kafka.producerConfig.schemaRegistry.schema.value={"namespace": "example.wikipedia.avro","type": "record","name": "WikipediaArticle","fields": [{"name": "pageid", "type": ["double", "null"]},{"name": "title", "type": ["string", "null"]},{"name": "user", "type": ["string", "null"]},{"name": "anon", "type": ["string", "null"]},{"name": "userid",  "type": ["double", "null"]},{"name": "timestamp", "type": ["string", "null"]},{"name": "size",  "type": ["double", "null"]},{"name": "contentformat",  "type": ["string", "null"]},{"name": "contentmodel",  "type": ["string", "null"]},{"name": "content", "type": ["string", "null"]}]}
```


* Run the standalone launcher with the wikipedia-kafka.pull file. You should see something like this. 

```
INFO  [TaskExecutor-0] gobblin.example.wikipedia.WikipediaExtractor  243 - 5 record(s) retrieved for title LinkedIn
INFO  [TaskExecutor-0] gobblin.example.wikipedia.WikipediaExtractor  243 - 5 record(s) retrieved for title Parris_Cues
INFO  [TaskExecutor-0] gobblin.example.wikipedia.WikipediaExtractor  243 - 5 record(s) retrieved for title Barbara_Corcoran
INFO  [TaskExecutor-0] gobblin.runtime.Task  176 - Extracted 20 data records
INFO  [TaskExecutor-0] gobblin.runtime.Task  177 - Row quality checker finished with results:
INFO  [TaskExecutor-0] gobblin.publisher.TaskPublisher  43 - All components finished successfully, checking quality tests
INFO  [TaskExecutor-0] gobblin.publisher.TaskPublisher  45 - All required test passed for this task passed.
INFO  [TaskExecutor-0] gobblin.publisher.TaskPublisher  47 - Cleanup for task publisher executed successfully.
INFO  [TaskExecutor-0] gobblin.runtime.Fork  261 - Committing data for fork 0 of task task_PullFromWikipediaToKafka_1472246706122_0
INFO  [TaskExecutor-0] gobblin.kafka.writer.KafkaDataWriter  211 - Successfully committed 20 records.
```

* To verify that the records have indeed been ingested into Kafka, you can run a kafka console consumer or run Gobblin's [kafka-console pull file](https://github.com/linkedin/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia-kafka.pull) which prints the events from Kafka onto the console.

#Configuration Details

The Kafka writer supports all the configuration parameters supported by the [0.8.2 Java Kafka Producer](http://kafka.apache.org/082/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html). All you have to do is prefix `writer.kafka.producerConfig.` to each configuration property that the producer supports. For example, if you want to set the `acks` parameter to `all` to ensure full acknowledgement of writes, you would set `writer.kafka.producerConfig.acks=all` in your pull file. For a comprehensive list of all the configuration properties supported by the producers, go through the [official documentation here](http://kafka.apache.org/082/documentation.html#newproducerconfigs). Note: Since Gobblin is currently built against Kafka 0.8.2, the configuration options apply to the new 0.8.2 java producer.

There are a few key parameters at the Gobblin level that control the behavior of the data writer. 

| Property Name | Semantics | 
|---|---|
| `writer.kafka.topic` | The topic that the writer will be writing to. At this time, the writer can only write to a single topic per pipeline. | 
| `writer.kafka.failureAllowancePercentage` | The percentage of failures that you are willing to tolerate while writing to Kafka. Gobblin will mark the workunit successful and move on if there are failures but not enough to trip the failure threshold. Only successfully acknowledged writes are counted as successful, all others are considered as failures. The default for the failureAllowancePercentage is set to 20.0. This means that as long as 80% of the data is acknowledged by Kafka, Gobblin will move on. If you want higher guarantees, set this config value to a lower value. e.g. If you want 99% delivery guarantees, set this value to 1.0 |
| `writer.kafka.commitTimeoutMillis` | The amount of time that the Gobblin committer will wait before abandoning its wait for unacknowledged writes. This defaults to 1 minute. | 

#What Next?

You can now set up Kafka as the destination for any of your sources. All you have to do is set up the writer configuration correctly in your pull files. Happy Ingesting!



