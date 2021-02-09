

Problem Statement
=================

Current Gobblin Kafka [`High Level Consumer`](https://github.com/apache/gobblin/blob/master/gobblin-runtime/src/main/java/org/apache/gobblin/runtime/kafka/HighLevelConsumer.java) uses Kafka Consumer (0.8) APIs and gobblin support for them will be deprecated. The Re-design's primary goal is to replace old kafka consumer APIs like [`ConsumerConnector`](https://archive.apache.org/dist/kafka/0.8.2.2/scaladoc/index.html#kafka.consumer.ConsumerConnector) and [`MessageAndMetadata`](https://archive.apache.org/dist/kafka/0.8.2.2/scaladoc/index.html#kafka.message.MessageAndMetadata) with a consumer abstraction [`GobblinKafkaConsumerClient`](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-kafka-common/src/main/java/org/apache/gobblin/kafka/client/GobblinKafkaConsumerClient.java). 
Additionally, the old design uses kafka auto commit feature which can cause potential loss of messages when offsets are committed and the system fails before messages are processed.

Detailed design and implementation details can be found [here](https://cwiki.apache.org/confluence/display/GOBBLIN/GIP+5%3A+High+Level+Consumer+Re-design)

New Design & Details 
====================

GobblinKafkaConsumerClient

The new design uses [`GobblinKafkaConsumerClient`](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-kafka-common/src/main/java/org/apache/gobblin/kafka/client/GobblinKafkaConsumerClient.java) which is a simplified, generic wrapper client to communicate with Kafka. This class does not depend on classes defined in kafka-clients library. This allows the high level consumer to work with different versions of kafka. Concrete classes implementing this interface use a specific version of kafka-client library. See [`Kafka09ConsumerClient`](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-kafka-09/src/main/java/org/apache/gobblin/kafka/client/Kafka09ConsumerClient.java)


Manual Offset Commit

`GobblinKafkaConsumerClient` API has been enhanced to allow manual committing of offsets.

``` java 
  /**
   * Commit offsets manually to Kafka asynchronously
   */
  default void commitOffsetsAsync(Map<KafkaPartition, Long> partitionOffsets) {
    return;
  }

  /**
   * Commit offsets manually to Kafka synchronously
   */
  default void commitOffsetsSync(Map<KafkaPartition, Long> partitionOffsets) {
    return;
  }

  /**
   * returns the last committed offset for a KafkaPartition
   * @param partition
   * @return last committed offset or -1 for invalid KafkaPartition
   */
  default long committed(KafkaPartition partition) {
    return -1L;
  }
```

High level consumer records topic partitions and their offsets AFTER the messages are processed and commits them periodically to kafka. This ensures at-least once delivery in case of a failure.

Additionally, APIs are provided to subscribe to a topic along with a [`GobblinKafkaRebalanceListener`](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-kafka-common/src/main/java/org/apache/gobblin/kafka/client/GobblinConsumerRebalanceListener.java) that provides hooks to when a consumer joins/leaves a consumer group.
In this case, we commit remaining offsets and clear offset caches.

``` java 
  /**
   * Subscribe to a topic
   * @param topic
   */
  default void subscribe(String topic) {
    return;
  }

  /**
   * Subscribe to a topic along with a GobblinKafkaRebalanceListener
   * @param topic
   */
  default void subscribe(String topic, GobblinConsumerRebalanceListener listener) {
    return;
  }
```