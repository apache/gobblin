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

package gobblin.converter;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.KafkaSchemaRegistryFactory;
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

  public static final String SCHEMA_KEY = "testKey";

  private GenericRecord mockInputRecord = mock(GenericRecord.class);
  private GenericRecord mockOutputRecord = mock(GenericRecord.class);
  public static Schema mockSchema = mock(Schema.class);

  class EnvelopeSchemaConverterForTest extends EnvelopeSchemaConverter {
    @Override
    public byte[] getPayload(GenericRecord inputRecord, String payloadFieldName) {
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
    when(mockInputRecord.get("payloadSchemaId")).thenReturn(SCHEMA_KEY);
    when(mockOutputRecord.getSchema()).thenReturn(mockSchema);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "testEvent");
    workUnitState.setProp("kafka.schema.registry.url", "testUrl");
    workUnitState.setProp(KafkaSchemaRegistryFactory.KAFKA_SCHEMA_REGISTRY_FACTORY_CLASS,
        KafkaAvroSchemaRegistryForTest.Factory.class.getName());

    EnvelopeSchemaConverterForTest converter = new EnvelopeSchemaConverterForTest();
    converter.init(workUnitState);
    GenericRecord output = converter.convertRecord(null, mockInputRecord, workUnitState).iterator().next();
    Assert.assertEquals(output, mockOutputRecord);
  }
}
