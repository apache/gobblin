/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.source.extractor.extract.kafka;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import kafka.message.Message;
import kafka.message.MessageAndOffset;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.kafka.client.Kafka08ConsumerClient.Kafka08ConsumerRecord;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaDeserializerExtractor.Deserializers;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.PropertiesUtils;


@Test(groups = { "gobblin.source.extractor.extract.kafka" })
public class KafkaDeserializerExtractorTest {

  private static final String TEST_TOPIC_NAME = "testTopic";
  private static final String TEST_URL = "testUrl";
  private static final String TEST_RECORD_NAME = "testRecord";
  private static final String TEST_NAMESPACE = "testNamespace";
  private static final String TEST_FIELD_NAME = "testField";
  private static final String TEST_FIELD_NAME2 = "testField2";

  @Test
  public void testDeserializeRecord() throws IOException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();

    String testString = "Hello World";
    ByteBuffer testStringByteBuffer = ByteBuffer.wrap(testString.getBytes(StandardCharsets.UTF_8));

    Deserializer<Object> mockKafkaDecoder = mock(Deserializer.class);
    KafkaSchemaRegistry<?, ?> mockKafkaSchemaRegistry = mock(KafkaSchemaRegistry.class);
    when(mockKafkaDecoder.deserialize(TEST_TOPIC_NAME, testStringByteBuffer.array())).thenReturn(testString);

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState,
        Optional.fromNullable(Deserializers.BYTE_ARRAY),
        mockKafkaDecoder, mockKafkaSchemaRegistry);

    ByteArrayBasedKafkaRecord mockMessageAndOffset = getMockMessageAndOffset(testStringByteBuffer);

    Assert.assertEquals(kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset), testString);
  }

  @Test
  public void testBuiltInStringDeserializer() throws ReflectiveOperationException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();
    mockWorkUnitState.setProp(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE,
        KafkaDeserializerExtractor.Deserializers.STRING.name());

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState);

    Assert.assertEquals(kafkaDecoderExtractor.getKafkaDeserializer().getClass(),
        KafkaDeserializerExtractor.Deserializers.STRING.getDeserializerClass());
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaSchemaRegistry().getClass(),
        KafkaDeserializerExtractor.Deserializers.STRING.getSchemaRegistryClass());
  }

  @Test
  public void testBuiltInGsonDeserializer() throws ReflectiveOperationException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();
    mockWorkUnitState.setProp(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE,
        KafkaDeserializerExtractor.Deserializers.GSON.name());

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState);

    Assert.assertEquals(kafkaDecoderExtractor.getKafkaDeserializer().getClass(),
        KafkaDeserializerExtractor.Deserializers.GSON.getDeserializerClass());
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaSchemaRegistry().getClass(),
        KafkaDeserializerExtractor.Deserializers.GSON.getSchemaRegistryClass());
  }

  @Test
  public void testBuiltInConfluentAvroDeserializer() throws ReflectiveOperationException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();
    mockWorkUnitState.setProp(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE,
        KafkaDeserializerExtractor.Deserializers.CONFLUENT_AVRO.name());

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState) {
      @Override
      public Object getSchema() {
        return SchemaBuilder.record(TEST_RECORD_NAME)
            .namespace(TEST_NAMESPACE).fields()
            .name(TEST_FIELD_NAME).type().stringType().noDefault()
            .endRecord();
      }
    };

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

    Schema schema = SchemaBuilder.record(TEST_RECORD_NAME)
        .namespace(TEST_NAMESPACE).fields()
        .name(TEST_FIELD_NAME).type().stringType().noDefault()
        .endRecord();

    GenericRecord testGenericRecord = new GenericRecordBuilder(schema).set(TEST_FIELD_NAME, "testValue").build();

    SchemaRegistryClient mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
    when(mockSchemaRegistryClient.getByID(any(Integer.class))).thenReturn(schema);

    Serializer<Object> kafkaEncoder = new KafkaAvroSerializer(mockSchemaRegistryClient);
    Deserializer<Object> kafkaDecoder = new KafkaAvroDeserializer(mockSchemaRegistryClient);

    ByteBuffer testGenericRecordByteBuffer =
        ByteBuffer.wrap(kafkaEncoder.serialize(TEST_TOPIC_NAME, testGenericRecord));

    KafkaSchemaRegistry<Integer, Schema> mockKafkaSchemaRegistry = mock(KafkaSchemaRegistry.class);
    KafkaDeserializerExtractor kafkaDecoderExtractor =
        new KafkaDeserializerExtractor(mockWorkUnitState,
            Optional.fromNullable(Deserializers.CONFLUENT_AVRO), kafkaDecoder, mockKafkaSchemaRegistry);

    ByteArrayBasedKafkaRecord mockMessageAndOffset = getMockMessageAndOffset(testGenericRecordByteBuffer);

    Assert.assertEquals(kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset), testGenericRecord);
  }

  @Test
  public void testConfluentAvroDeserializerForSchemaEvolution() throws IOException, RestClientException, SchemaRegistryException {
    WorkUnitState mockWorkUnitState = getMockWorkUnitState();
    mockWorkUnitState.setProp("schema.registry.url", TEST_URL);

    Schema schemaV1 = SchemaBuilder.record(TEST_RECORD_NAME)
        .namespace(TEST_NAMESPACE).fields()
        .name(TEST_FIELD_NAME).type().stringType().noDefault()
        .endRecord();

    Schema schemaV2 = SchemaBuilder.record(TEST_RECORD_NAME)
        .namespace(TEST_NAMESPACE).fields()
        .name(TEST_FIELD_NAME).type().stringType().noDefault()
        .optionalString(TEST_FIELD_NAME2).endRecord();

    GenericRecord testGenericRecord = new GenericRecordBuilder(schemaV1).set(TEST_FIELD_NAME, "testValue").build();

    SchemaRegistryClient mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
    when(mockSchemaRegistryClient.getByID(any(Integer.class))).thenReturn(schemaV1);

    Serializer<Object> kafkaEncoder = new KafkaAvroSerializer(mockSchemaRegistryClient);
    Deserializer<Object> kafkaDecoder = new KafkaAvroDeserializer(mockSchemaRegistryClient);

    ByteBuffer testGenericRecordByteBuffer =
        ByteBuffer.wrap(kafkaEncoder.serialize(TEST_TOPIC_NAME, testGenericRecord));

    KafkaSchemaRegistry<Integer, Schema> mockKafkaSchemaRegistry = mock(KafkaSchemaRegistry.class);
    when(mockKafkaSchemaRegistry.getLatestSchemaByTopic(TEST_TOPIC_NAME)).thenReturn(schemaV2);

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState,
        Optional.fromNullable(Deserializers.CONFLUENT_AVRO), kafkaDecoder, mockKafkaSchemaRegistry);
    when(kafkaDecoderExtractor.getSchema()).thenReturn(schemaV2);

    ByteArrayBasedKafkaRecord mockMessageAndOffset = getMockMessageAndOffset(testGenericRecordByteBuffer);

    GenericRecord received = (GenericRecord) kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset);
    Assert.assertEquals(received.toString(), "{\"testField\": \"testValue\", \"testField2\": null}");

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
        new KafkaDeserializerExtractor(mockWorkUnitState,
            Optional.fromNullable(Deserializers.CONFLUENT_JSON), kafkaDecoder, mockKafkaSchemaRegistry);

    ByteArrayBasedKafkaRecord mockMessageAndOffset = getMockMessageAndOffset(testKafkaRecordByteBuffer);
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

  private ByteArrayBasedKafkaRecord getMockMessageAndOffset(ByteBuffer payload) {
    MessageAndOffset mockMessageAndOffset = mock(MessageAndOffset.class);
    Message mockMessage = mock(Message.class);
    when(mockMessage.payload()).thenReturn(payload);
    when(mockMessageAndOffset.message()).thenReturn(mockMessage);
    return new Kafka08ConsumerRecord(mockMessageAndOffset);
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