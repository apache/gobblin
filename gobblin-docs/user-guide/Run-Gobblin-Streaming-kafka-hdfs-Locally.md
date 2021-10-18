# Table of Contents

[TOC]

# Introduction

Gobblin supports streaming mode that allows continuous ingestion of data from Kafka to HDFS. The streaming mode has been deployed in production at LinkedIn as a Gobblin cluster that uses Yarn for container allocation and Helix for task coordination.

Here, we describe how to set up a Kafka -> HDFS pipeline in local mode for users to easily start and test out a streaming ingestion pipeline. 


# Setup local kafka cluster 

Follow [kafka quick start](https://kafka.apache.org/quickstart) to set up your kafka cluster, and create test topic "testEvents"

# Run EmbeddedGobblin to start the job

We use the configuration: /gobblin-modules/gobblin-kafka-09/src/test/resources/kafkaHDFSStreaming.conf to execute the job.

To run the job, in your intellij, you can run the test in /gobblin-modules/gobblin-kafka-09/src/test/java/org/apache/gobblin/kafka/KafkaStreamingLocalTest
by removing the line '(enabled=false)'. In order to run the test in IDE, you may need to manually delete log4j-over-slf4j jar in IDE 

Under your kafka dir, you can run following command to produce data into your kafka topic

`bin/kafka-console-producer.sh --topic testEvents --bootstrap-server localhost:9092`

The job will continually consume from testEvents and write out data as txt file onto your local fileSystem (/tmp/gobblin/kafka/publish). It will write put data every 60 seconds, and will never end until
you manually kill it.

If you want the job ingest data as avro/orc, you will need to have schema registry as schema source and change the job configuration to control the behavior, a sample configuration can be found [here](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-azkaban/src/main/resources/conf/gobblin_jobs/kafka-hdfs-streaming-avro.conf)

