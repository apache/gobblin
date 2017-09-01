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

package org.apache.gobblin.converter;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistryFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit test for {@link EnvelopeSchemaConverter}.
 */
@Test(groups = {"gobblin.converter"})
public class EnvelopeSchemaConverterTest {
  public static final String TEST_SCHEMA_STR =
      "{ \"type\":\"record\", \"name\":\"TestRecord\", \"namespace\":\"org.apache.gobblin.test\", \"fields\":[ { \"name\":\"metadata\", \"type\":{ \"type\":\"map\", \"values\":\"string\" }, \"doc\":\"record metadata.\" }, { \"name\":\"key\", \"type\":\"bytes\", \"doc\":\"serialized key.\" }, { \"name\":\"payload\", \"type\":\"bytes\", \"doc\":\"serialized payload data.\" }, { \"name\":\"nestedRecord1\", \"type\":{ \"type\":\"record\", \"name\":\"NestedRecord1\", \"namespace\":\"org.apache.gobblin.test\", \"fields\":[ { \"name\":\"time\", \"type\":\"long\", \"doc\":\"a time stamp.\" }, { \"name\":\"id\", \"type\":\"string\", \"doc\":\"ID of the record.\" } ] }, \"doc\":\"nested record 1\" } ] }";
  public static final String SCHEMA_KEY = "testKey";

  private GenericRecord mockOutputRecord = mock(GenericRecord.class);
  public static Schema mockSchema = mock(Schema.class);

  class EnvelopeSchemaConverterForTest extends EnvelopeSchemaConverter {
    @Override
    public byte[] getPayload(GenericRecord inputRecord, String payloadFieldName) throws DataConversionException {
      return null;
    }

    @Override
    public GenericRecord deserializePayload(byte[] payload, Schema payloadSchema) {
      Assert.assertEquals(payloadSchema, mockSchema);
      return mockOutputRecord;
    }
  }

  @Test
  public void convertRecordTest() throws Exception {
    Schema inputSchema = new Schema.Parser().parse(TEST_SCHEMA_STR);
    GenericRecord inputRecord = new GenericData.Record(inputSchema);
    Map<Object, Object> metadata = new HashMap<>();
    metadata.put(new Utf8("payloadSchemaId"), SCHEMA_KEY);
    inputRecord.put("metadata", metadata);

    when(mockOutputRecord.getSchema()).thenReturn(mockSchema);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "testEvent");
    workUnitState.setProp(EnvelopeSchemaConverter.PAYLOAD_SCHEMA_ID_FIELD, "metadata.payloadSchemaId");
    workUnitState.setProp("kafka.schema.registry.url", "testUrl");
    workUnitState.setProp(KafkaSchemaRegistryFactory.KAFKA_SCHEMA_REGISTRY_FACTORY_CLASS,
        KafkaAvroSchemaRegistryForTest.Factory.class.getName());

    EnvelopeSchemaConverterForTest converter = new EnvelopeSchemaConverterForTest();
    converter.init(workUnitState);
    GenericRecord output = converter.convertRecord(null, inputRecord, workUnitState).iterator().next();
    Assert.assertEquals(output, mockOutputRecord);
  }
}
