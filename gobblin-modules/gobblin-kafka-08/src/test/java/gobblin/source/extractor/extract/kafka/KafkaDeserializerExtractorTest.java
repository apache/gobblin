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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.PropertiesUtils;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Test(groups = { "gobblin.source.extractor.extract.kafka" })
public class KafkaDeserializerExtractorTest {

  private static final String TEST_TOPIC_NAME = "testTopic";
  private static final String TEST_URL = "testUrl";
  private static final String TEST_RECORD_NAME = "testRecord";
  private static final String TEST_NAMESPACE = "testNamespace";
  private static final String TEST_FIELD_NAME = "testField";

  @Test
  public void testDeserializeRecord() throws IOException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();

    String testString = "Hello World";
    ByteBuffer testStringByteBuffer = ByteBuffer.wrap(testString.getBytes(StandardCharsets.UTF_8));

    Deserializer<Object> mockKafkaDecoder = mock(Deserializer.class);
    KafkaSchemaRegistry<?, ?> mockKafkaSchemaRegistry = mock(KafkaSchemaRegistry.class);
    when(mockKafkaDecoder.deserialize(TEST_TOPIC_NAME, testStringByteBuffer.array())).thenReturn(testString);

    KafkaDeserializerExtractor kafkaDecoderExtractor =
        new KafkaDeserializerExtractor(mockWorkUnitState, mockKafkaDecoder, mockKafkaSchemaRegistry);

    MessageAndOffset mockMessageAndOffset = getMockMessageAndOffset(testStringByteBuffer);

    Assert.assertEquals(kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset), testString);
  }

  @Test
  public void testBuiltInDeserializers() throws ReflectiveOperationException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();
    mockWorkUnitState.setProp(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE,
        KafkaDeserializerExtractor.Deserializers.CONFLUENT_AVRO.name());

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState);

    Assert.assertEquals(kafkaDecoderExtractor.getKafkaDeserializer().getClass(),
        KafkaDeserializerExtractor.Deserializers.CONFLUENT_AVRO.getDeserializerClass());
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaSchemaRegistry().getClass(),
        KafkaDeserializerExtractor.Deserializers.CONFLUENT_AVRO.getSchemaRegistryClass());
  }

  @Test
  public void testCustomDeserializer() throws ReflectiveOperationException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();
    mockWorkUnitState
        .setProp(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE, KafkaJsonDeserializer.class.getName());
    mockWorkUnitState
        .setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, SimpleKafkaSchemaRegistry.class.getName());

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState);
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaDeserializer().getClass(), KafkaJsonDeserializer.class);
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaSchemaRegistry().getClass(), SimpleKafkaSchemaRegistry.class);
  }

  @Test
  public void testConfluentAvroDeserializer() throws IOException, RestClientException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();
    mockWorkUnitState.setProp("schema.registry.url", TEST_URL);

    Schema schema =
        SchemaBuilder.record(TEST_RECORD_NAME).namespace(TEST_NAMESPACE).fields().name(TEST_FIELD_NAME).type()
            .stringType().noDefault().endRecord();
    GenericRecord testGenericRecord = new GenericRecordBuilder(schema).set(TEST_FIELD_NAME, "testValue").build();

    SchemaRegistryClient mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
    when(mockSchemaRegistryClient.getByID(any(Integer.class))).thenReturn(schema);

    Serializer<Object> kafkaEncoder = new KafkaAvroSerializer(mockSchemaRegistryClient);
    Deserializer<Object> kafkaDecoder = new KafkaAvroDeserializer(mockSchemaRegistryClient);

    ByteBuffer testGenericRecordByteBuffer =
        ByteBuffer.wrap(kafkaEncoder.serialize(TEST_TOPIC_NAME, testGenericRecord));

    KafkaSchemaRegistry<?, ?> mockKafkaSchemaRegistry = mock(KafkaSchemaRegistry.class);
    KafkaDeserializerExtractor kafkaDecoderExtractor =
        new KafkaDeserializerExtractor(mockWorkUnitState, kafkaDecoder, mockKafkaSchemaRegistry);

    MessageAndOffset mockMessageAndOffset = getMockMessageAndOffset(testGenericRecordByteBuffer);

    Assert.assertEquals(kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset), testGenericRecord);
  }

  @Test
  public void testConfluentJsonDeserializer() throws IOException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();
    mockWorkUnitState.setProp("json.value.type", KafkaRecord.class.getName());

    KafkaRecord testKafkaRecord = new KafkaRecord("Hello World");

    Serializer<KafkaRecord> kafkaEncoder = new KafkaJsonSerializer<>();
    kafkaEncoder.configure(PropertiesUtils.propsToStringKeyMap(mockWorkUnitState.getProperties()), false);

    Deserializer<KafkaRecord> kafkaDecoder = new KafkaJsonDeserializer<>();
    kafkaDecoder.configure(PropertiesUtils.propsToStringKeyMap(mockWorkUnitState.getProperties()), false);

    ByteBuffer testKafkaRecordByteBuffer = ByteBuffer.wrap(kafkaEncoder.serialize(TEST_TOPIC_NAME, testKafkaRecord));

    KafkaSchemaRegistry<?, ?> mockKafkaSchemaRegistry = mock(KafkaSchemaRegistry.class);
    KafkaDeserializerExtractor kafkaDecoderExtractor =
        new KafkaDeserializerExtractor(mockWorkUnitState, kafkaDecoder, mockKafkaSchemaRegistry);

    MessageAndOffset mockMessageAndOffset = getMockMessageAndOffset(testKafkaRecordByteBuffer);
    Assert.assertEquals(kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset), testKafkaRecord);
  }

  private WorkUnitState getMockWorkUnitState() {
    WorkUnit mockWorkUnit = WorkUnit.createEmpty();
    mockWorkUnit.setWatermarkInterval(new WatermarkInterval(new MultiLongWatermark(new ArrayList<Long>()),
        new MultiLongWatermark(new ArrayList<Long>())));

    WorkUnitState mockWorkUnitState = new WorkUnitState(mockWorkUnit, new State());
    mockWorkUnitState.setProp(KafkaSource.TOPIC_NAME, TEST_TOPIC_NAME);
    mockWorkUnitState.setProp(KafkaSource.PARTITION_ID, "1");
    mockWorkUnitState.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:8080");
    mockWorkUnitState.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL, TEST_URL);

    return mockWorkUnitState;
  }

  private MessageAndOffset getMockMessageAndOffset(ByteBuffer payload) {
    MessageAndOffset mockMessageAndOffset = mock(MessageAndOffset.class);
    Message mockMessage = mock(Message.class);
    when(mockMessage.payload()).thenReturn(payload);
    when(mockMessageAndOffset.message()).thenReturn(mockMessage);
    return mockMessageAndOffset;
  }

  @AllArgsConstructor
  @NoArgsConstructor
  @EqualsAndHashCode
  @Getter
  @Setter
  private static class KafkaRecord {

    private String value;
  }
}
