# Table of Contents

[TOC]

# Introduction

Gobblin streaming enable user to ingest data from Kafka to HDFS in streaming manner. Normally it's running on yarn, to get the full function, 
you need have helix cluster, yarn cluster, kafka server and schema registry set up. But we also have one local mode of streaming task using EmbeddedGobblin
for user to easily start and test out the job. Here is the steps:

# Setup local kafka cluster 

Just follow [kafka quick start](https://kafka.apache.org/quickstart) to set up your kafka cluster, and create test topic "testEvents"

# Run EmbeddedGobblin to start the job

We are using configuration: /gobblin-modules/gobblin-kafka-09/src/test/resources/kafkaHDFSStreaming.conf to execute the job.

To run the job, in your intellij, you can run the test in /gobblin-modules/gobblin-kafka-09/src/test/java/org/apache/gobblin/kafka/KafkaStreamingLocalTest
by remove the line '(enabled=false)'. In order to run the test in IDE, you may need to manually delete log4j-over-slf4j jar in IDE 

Under your kafka dir, you can run following command to produce data into your kafka topic

`bin/kafka-console-producer.sh --topic testEvents --bootstrap-server localhost:9092`

The job will continually consume from testEvents and write out data as txt file onto your local fileSystem (/tmp/gobblin/kafka/publish). It will write put data every 60 seconds, and will never end until
you manually kill it.

If you want the job ingest data as avro/orc, you will need to have schema registry as schema source and change the job configuration to control the behavior

