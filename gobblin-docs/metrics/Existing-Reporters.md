Table of Contents
-----------------

[TOC]

Metric Reporters
================

* [Output Stream Reporter](https://github.com/apache/gobblin/blob/master/gobblin-metrics-libs/gobblin-metrics-base/src/main/java/org/apache/gobblin/metrics/reporter/OutputStreamReporter.java): allows printing metrics to any OutputStream, including STDOUT and files.
* [Kafka Reporter](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-kafka-common/src/main/java/org/apache/gobblin/metrics/kafka/KafkaReporter.java): emits metrics to Kafka topic as Json messages.
* [Kafka Avro Reporter](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-kafka-common/src/main/java/org/apache/gobblin/metrics/kafka/KafkaAvroReporter.java): emits metrics to Kafka topic as Avro messages with schema [MetricReport](https://github.com/apache/gobblin/blob/master/gobblin-metrics-libs/gobblin-metrics-base/src/main/avro/MetricReport.avsc).
* [Graphite Reporter](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-metrics-graphite/src/main/java/org/apache/gobblin/metrics/graphite/GraphiteReporter.java): emits metrics to Graphite. This reporter has a different, deprecated construction API included in its javadoc.
* [Influx DB Reporter](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-metrics-influxdb/src/main/java/org/apache/gobblin/metrics/influxdb/InfluxDBReporter.java): emits metrics to Influx DB. This reporter has a different, deprecated construction API included in its javadoc.
* [Hadoop Counter Reporter](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-metrics-hadoop/src/main/java/org/apache/gobblin/metrics/hadoop/HadoopCounterReporter.java): emits metrics as Hadoop counters at the end of the execution. Available for old and new Hadoop API. This reporter has a different, deprecated construction API included in its javadoc. Due to limits on the number of Hadoop counters that can be created, this reporter is not recommended except for applications with very few metrics.

Event Reporters
===============
* [Output Stream Event Reporter](https://github.com/apache/gobblin/blob/master/gobblin-metrics-libs/gobblin-metrics-base/src/main/java/org/apache/gobblin/metrics/reporter/OutputStreamEventReporter.java): Emits events to any output stream, including STDOUT and files.
* [Kafka Event Reporter](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-kafka-common/src/main/java/org/apache/gobblin/metrics/kafka/KafkaEventReporter.java): Emits events to Kafka topic as Json messages.
* [Kafka Avro Event Reporter](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-kafka-common/src/main/java/org/apache/gobblin/metrics/kafka/KafkaAvroEventReporter.java): Emits events to Kafka topic as Avro messages using the schema [GobblinTrackingEvent](https://github.com/apache/gobblin/blob/master/gobblin-metrics-libs/gobblin-metrics-base/src/main/avro/GobblinTrackingEvent.avsc).
