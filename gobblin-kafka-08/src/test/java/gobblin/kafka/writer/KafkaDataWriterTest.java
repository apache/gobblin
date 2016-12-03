/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.kafka.writer;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import kafka.consumer.ConsumerIterator;
import lombok.extern.slf4j.Slf4j;

import gobblin.kafka.KafkaTestBase;
import gobblin.kafka.schemareg.CachingKafkaSchemaRegistry;
import gobblin.kafka.schemareg.ConfigDrivenMd5SchemaRegistry;
import gobblin.kafka.schemareg.KafkaSchemaRegistry;
import gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import gobblin.kafka.schemareg.LiKafkaSchemaRegistry;
import gobblin.kafka.schemareg.SchemaRegistryException;
import gobblin.kafka.serialize.LiAvroDeserializer;


@Slf4j
public class KafkaDataWriterTest {


  private final KafkaTestBase _kafkaTestHelper;
  public KafkaDataWriterTest()
      throws InterruptedException, RuntimeException {
    _kafkaTestHelper = new KafkaTestBase();
  }

  @BeforeSuite
  public void beforeSuite() {
    log.warn("Process id = " + ManagementFactory.getRuntimeMXBean().getName());

    _kafkaTestHelper.startServers();
  }

  @AfterSuite
  public void afterSuite()
      throws IOException {
    try {
      _kafkaTestHelper.stopClients();
    }
    finally {
      _kafkaTestHelper.stopServers();
    }
  }

  @Test
  public void testStringSerialization()
      throws IOException, InterruptedException {
    String topic = "testStringSerialization";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    KafkaDataWriter<String> kafkaWriter = new KafkaDataWriter<String>(props);

    String messageString = "foobar";
    try {
      kafkaWriter.write(messageString);
      kafkaWriter.commit();
    }
    finally
    {
      kafkaWriter.close();
    }

    Assert.assertEquals(kafkaWriter.recordsWritten(), 1);
    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
    String messageReceived = new String(message);
    Assert.assertEquals(messageReceived, messageString);

  }

  @Test
  public void testBinarySerialization()
      throws IOException, InterruptedException {
    String topic = "testBinarySerialization";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    KafkaDataWriter<byte[]> kafkaWriter = new KafkaDataWriter<>(props);

    byte[] messageBytes = KafkaTestUtils.generateRandomBytes();

    try {
      kafkaWriter.write(messageBytes);
      kafkaWriter.commit();
    }
    finally
    {
      kafkaWriter.close();
    }

    Assert.assertEquals(kafkaWriter.recordsWritten(), 1);
    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
    Assert.assertEquals(message, messageBytes);
  }

  @Test
  public void testAvroSerialization()
      throws IOException, InterruptedException, SchemaRegistryException {
    String topic = "testAvroSerialization";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "value.serializer",
        "gobblin.kafka.serialize.LiAvroSerializer");

    // set up mock schema registry

    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX
        + KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS,
        ConfigDrivenMd5SchemaRegistry.class.getCanonicalName());

    KafkaDataWriter<GenericRecord> kafkaWriter = new KafkaDataWriter<>(props);

    GenericRecord record = KafkaTestUtils.generateRandomAvroRecord();
    try {
      kafkaWriter.write(record);
      kafkaWriter.commit();
    }
    finally
    {
      kafkaWriter.close();
    }

    Assert.assertEquals(kafkaWriter.recordsWritten(), 1);
    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
    ConfigDrivenMd5SchemaRegistry schemaReg = new ConfigDrivenMd5SchemaRegistry(topic, record.getSchema());
    LiAvroDeserializer deser = new LiAvroDeserializer(schemaReg);
    GenericRecord receivedRecord = deser.deserialize(topic, message);
    System.out.println(record.toString());
    System.out.println(receivedRecord.toString());
  }

  //@Test
  public void testRandomThing() throws Exception {

    System.out.println("Kafka server port = " + _kafkaTestHelper.getKafkaServerPort());

    Properties props = new Properties();
    props.setProperty(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_URL,
        "http://ltx1-schemaregistry-vip-2.stg.linkedin.com:10252/schemaRegistry/schemas");
    LiKafkaSchemaRegistry schemaReg = new LiKafkaSchemaRegistry(props);
    final KafkaSchemaRegistry cSchemaReg = new CachingKafkaSchemaRegistry(schemaReg);

    Runnable messageReader = new Runnable() {
      @Override
      public void run() {
        String topic = "EITestAdImpressionEventPush11142016";
        _kafkaTestHelper.provisionTopic(topic);
        final ConsumerIterator<byte[], byte[]> messageIterator = _kafkaTestHelper.getIteratorForTopic(topic);
        int messageNo = 0;
        LiAvroDeserializer deserializer = new LiAvroDeserializer(cSchemaReg);
        while (messageIterator.hasNext())
        {
          byte[] message = messageIterator.next().message();
          GenericRecord record = deserializer.deserialize(topic, message);
          System.out.println("Message " + messageNo++ + ":" + record.get("auditHeader").toString());
        }

      }
    };

    Runnable auditReader = new Runnable() {
      @Override
      public void run() {
        String auditTopic = "TrackingMonitoringEvent";
        _kafkaTestHelper.provisionTopic(auditTopic);
        final ConsumerIterator<byte[], byte[]> auditIterator = _kafkaTestHelper.getIteratorForTopic(auditTopic);
        int messageNo = 0;
        LiAvroDeserializer deserializer = new LiAvroDeserializer(cSchemaReg);

        while (auditIterator.hasNext())
        {
          byte[] message = auditIterator.next().message();
          GenericRecord record = deserializer.deserialize(auditTopic, message);
          System.out.println("AuditMessage " + messageNo++ + ":" + record.toString());
        }

      }

    };

    Thread messageThread = new Thread(messageReader);
    messageThread.start();

    Thread auditThread = new Thread(auditReader);
    auditThread.start();

    while (true)
    {
      Thread.sleep(1000);
    }

    /**
    String topic = "testAvroSerialization";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "value.serializer",
        "gobblin.kafka.serialize.LiAvroSerializer");

    // set up mock schema registry

    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX
            + KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS,
        ConfigDrivenMd5SchemaRegistry.class.getCanonicalName());

    KafkaDataWriter<GenericRecord> kafkaWriter = new KafkaDataWriter<>(props);

    GenericRecord record = KafkaTestUtils.generateRandomAvroRecord();
    try {
      kafkaWriter.write(record);
      kafkaWriter.commit();
    }
    finally
    {
      kafkaWriter.close();
    }

    Assert.assertEquals(kafkaWriter.recordsWritten(), 1);
    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
    ConfigDrivenMd5SchemaRegistry schemaReg = new ConfigDrivenMd5SchemaRegistry(topic, record.getSchema());
    LiAvroDeserializer deser = new LiAvroDeserializer(schemaReg);
    GenericRecord receivedRecord = deser.deserialize(topic, message);
    System.out.println(record.toString());
    System.out.println(receivedRecord.toString());
     **/

  }

}
