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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

import gobblin.kafka.KafkaTestBase;
import gobblin.kafka.schemareg.ConfigDrivenMd5SchemaRegistry;
import gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import gobblin.kafka.schemareg.SchemaRegistryException;
import gobblin.kafka.serialize.LiAvroDeserializer;
import gobblin.kafka.serialize.LiAvroDeserializerBase;
import gobblin.test.TestUtils;


@Slf4j
public class Kafka08DataWriterTest {


  private final KafkaTestBase _kafkaTestHelper;
  public Kafka08DataWriterTest()
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
    String topic = "testStringSerialization08";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Kafka08DataWriter<String> kafka08DataWriter = new Kafka08DataWriter<String>(props);
    String messageString = "foobar";
    final AtomicBoolean gotCallback = new AtomicBoolean(false);
    final AtomicBoolean gotSuccess = new AtomicBoolean(false);
    final AtomicBoolean gotFailure = new AtomicBoolean(false);
    kafka08DataWriter.setDefaultCallback(new WriteCallback() {
      @Override
      public void onSuccess() {
        gotCallback.set(true);
        gotSuccess.set(true);
      }

      @Override
      public void onFailure(Exception exception) {
        gotCallback.set(true);
        gotFailure.set(true);
      }
    });

    try {
      kafka08DataWriter.asyncWrite(messageString);
    }
    finally
    {
      kafka08DataWriter.close();
    }

    Assert.assertEquals(gotCallback.get(), true);
    Assert.assertEquals(gotSuccess.get(), true);
    Assert.assertEquals(gotFailure.get(), false);
    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
    String messageReceived = new String(message);
    Assert.assertEquals(messageReceived, messageString);

  }

  @Test
  public void testBinarySerialization()
      throws IOException, InterruptedException {
    String topic = "testBinarySerialization08";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    Kafka08DataWriter<byte[]> kafka08DataWriter = new Kafka08DataWriter<byte[]>(props);
    final AtomicBoolean gotCallback = new AtomicBoolean(false);
    final AtomicBoolean gotSuccess = new AtomicBoolean(false);
    final AtomicBoolean gotFailure = new AtomicBoolean(false);
    kafka08DataWriter.setDefaultCallback(new WriteCallback() {
      @Override
      public void onSuccess() {
        gotCallback.set(true);
        gotSuccess.set(true);
      }

      @Override
      public void onFailure(Exception exception) {
        gotCallback.set(true);
        gotFailure.set(true);
      }
    });
    byte[] messageBytes = TestUtils.generateRandomBytes();

    try {
      kafka08DataWriter.asyncWrite(messageBytes);
    }
    finally
    {
      kafka08DataWriter.close();
    }

    Assert.assertEquals(gotCallback.get(), true);
    Assert.assertEquals(gotSuccess.get(), true);
    Assert.assertEquals(gotFailure.get(), false);
    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
    Assert.assertEquals(message, messageBytes);
  }

  @Test
  public void testAvroSerialization()
      throws IOException, InterruptedException, SchemaRegistryException {
    String topic = "testAvroSerialization08";
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

    Kafka08DataWriter<GenericRecord> kafka08DataWriter = new Kafka08DataWriter<>(props);

    final AtomicBoolean gotCallback = new AtomicBoolean(false);
    final AtomicBoolean gotSuccess = new AtomicBoolean(false);
    final AtomicBoolean gotFailure = new AtomicBoolean(false);
    kafka08DataWriter.setDefaultCallback(new WriteCallback() {
      @Override
      public void onSuccess() {
        gotCallback.set(true);
        gotSuccess.set(true);
      }

      @Override
      public void onFailure(Exception exception) {
        gotCallback.set(true);
        gotFailure.set(true);
      }
    });

    GenericRecord record = TestUtils.generateRandomAvroRecord();
    try {
      kafka08DataWriter.asyncWrite(record);
    }
    finally
    {
      kafka08DataWriter.close();
    }

    Assert.assertEquals(gotCallback.get(), true);
    Assert.assertEquals(gotSuccess.get(), true);
    Assert.assertEquals(gotFailure.get(), false);

    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
    ConfigDrivenMd5SchemaRegistry schemaReg = new ConfigDrivenMd5SchemaRegistry(topic, record.getSchema());
    LiAvroDeserializer deser = new LiAvroDeserializer(schemaReg);
    GenericRecord receivedRecord = deser.deserialize(topic, message);
    Assert.assertEquals(record.toString(), receivedRecord.toString());
  }



}
