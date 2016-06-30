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
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

import gobblin.kafka.KafkaTestBase;
import gobblin.kafka.schemareg.ConfigDrivenMd5SchemaRegistry;
import gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import gobblin.kafka.schemareg.SchemaRegistryException;
import gobblin.kafka.serialize.LiAvroDeserializer;


public class KafkaDataWriterTest extends KafkaTestBase {


  private static final String TOPIC = KafkaDataWriterTest.class.getName();
  public KafkaDataWriterTest()
      throws InterruptedException, RuntimeException {
    super(TOPIC);
  }
  @AfterClass
  public void after() {
    try {
      close();
    } catch(Exception e) {
      System.err.println("Failed to close Kafka server.");
    }
  }

  @AfterSuite
  public void afterSuite() {
    closeServer();
  }

  @Test
  public void testStringSerialization()
      throws IOException, InterruptedException {
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, TOPIC);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "localhost:" + this.kafkaPort);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    KafkaDataWriter<String> kafkaWriter = new KafkaDataWriter<String>(props);

    String messageString = "foobar";
    try {
      kafkaWriter.write(messageString);
    }
    finally
    {
      kafkaWriter.close();
    }

    Thread.sleep(500);
    byte[] message = iterator.next().message();
    String messageReceived = new String(message);
    Assert.assertEquals(messageReceived, messageString);
  }

  @Test
  public void testBinarySerialization()
      throws IOException, InterruptedException {
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, TOPIC);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "localhost:" + this.kafkaPort);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    KafkaDataWriter<byte[]> kafkaWriter = new KafkaDataWriter<>(props);

    byte[] messageBytes = generateRandomBytes();

    try {
      kafkaWriter.write(messageBytes);
    }
    finally
    {
      kafkaWriter.close();
    }

    Thread.sleep(500);
    byte[] message = iterator.next().message();
    Assert.assertEquals(message, messageBytes);
  }

  private byte[] generateRandomBytes() {
    Random rng = new Random();
    int length = rng.nextInt(200);
    byte[] messageBytes = new byte[length];
    rng.nextBytes(messageBytes);
    return messageBytes;
  }

  @Test
  public void testAvroSerialization()
      throws IOException, InterruptedException, SchemaRegistryException {
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, TOPIC);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers", "localhost:" + this.kafkaPort);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "value.serializer",
        "gobblin.kafka.serialize.LiAvroSerializer");

    // set up mock schema registry

    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS,
        ConfigDrivenMd5SchemaRegistry.class.getCanonicalName());

    KafkaDataWriter<GenericRecord> kafkaWriter = new KafkaDataWriter<>(props);

    GenericRecord record = generateRandomAvroRecord();
    try {
      kafkaWriter.write(record);
    }
    finally
    {
      kafkaWriter.close();
    }

    Thread.sleep(500);

    byte[] message = iterator.next().message();
    ConfigDrivenMd5SchemaRegistry schemaReg = new ConfigDrivenMd5SchemaRegistry(TOPIC, record.getSchema());
    LiAvroDeserializer deser = new LiAvroDeserializer(schemaReg);
    GenericRecord receivedRecord = deser.deserialize(TOPIC, message);
    System.out.println(record.toString());
    System.out.println(receivedRecord.toString());
  }

  private GenericRecord generateRandomAvroRecord() {

    ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
    String fieldName = "field1";
    Schema fieldSchema = Schema.create(Schema.Type.STRING);
    String docString = "doc";
    fields.add(new Schema.Field(fieldName, fieldSchema, docString, null));
    Schema schema = Schema.createRecord("name", docString, "test",false);
    schema.setFields(fields);

    Record record = new Record(schema);
    record.put("field1", "foobar");

    return record;

  }
}
