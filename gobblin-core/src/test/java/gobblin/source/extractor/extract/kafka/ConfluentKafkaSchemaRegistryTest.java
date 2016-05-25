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

package gobblin.source.extractor.extract.kafka;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;


@Test(groups = { "gobblin.source.extractor.extract.kafka" })
public class ConfluentKafkaSchemaRegistryTest {

  private static final String TEST_TOPIC_NAME = "testTopic";
  private static final String TEST_URL = "testUrl";
  private static final String TEST_RECORD_NAME = "testRecord";
  private static final String TEST_NAMESPACE = "testNamespace";
  private static final String TEST_FIELD_NAME = "testField";

  @Test
  public void testRegisterAndGetByKey() throws SchemaRegistryException {
    Properties properties = new Properties();
    properties.setProperty(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL, TEST_URL);

    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    KafkaSchemaRegistry<Integer, Schema> kafkaSchemaRegistry =
        new ConfluentKafkaSchemaRegistry(properties, schemaRegistryClient);

    Schema schema =
        SchemaBuilder.record(TEST_RECORD_NAME).namespace(TEST_NAMESPACE).fields().name(TEST_FIELD_NAME).type()
            .stringType().noDefault().endRecord();

    Integer id = kafkaSchemaRegistry.register(schema);
    Assert.assertEquals(schema, kafkaSchemaRegistry.getSchemaByKey(id));
  }

  @Test
  public void testRegisterAndGetLatest() throws SchemaRegistryException {
    Properties properties = new Properties();
    properties.setProperty(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL, TEST_URL);

    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    KafkaSchemaRegistry<Integer, Schema> kafkaSchemaRegistry =
        new ConfluentKafkaSchemaRegistry(properties, schemaRegistryClient);

    Schema schema1 =
        SchemaBuilder.record(TEST_RECORD_NAME + "1").namespace(TEST_NAMESPACE).fields().name(TEST_FIELD_NAME).type()
            .stringType().noDefault().endRecord();

    Schema schema2 =
        SchemaBuilder.record(TEST_RECORD_NAME + "2").namespace(TEST_NAMESPACE).fields().name(TEST_FIELD_NAME).type()
            .stringType().noDefault().endRecord();

    kafkaSchemaRegistry.register(schema1, TEST_TOPIC_NAME);
    kafkaSchemaRegistry.register(schema2, TEST_TOPIC_NAME);

    Assert.assertNotEquals(schema1, kafkaSchemaRegistry.getLatestSchemaByTopic(TEST_TOPIC_NAME));
    Assert.assertEquals(schema2, kafkaSchemaRegistry.getLatestSchemaByTopic(TEST_TOPIC_NAME));
  }
}
