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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistryFactory;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit test for {@link EnvelopePayloadExtractingConverter}.
 */
@Test(groups = {"gobblin.converter"})
public class EnvelopePayloadExtractingConverterTest {
  private static final KafkaSchemaRegistry mockRegistry = mock(KafkaSchemaRegistry.class);

  @Test
  public void testConverter()
      throws Exception {
    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/envelope.avsc"));
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(inputSchema);

    File tmp = File.createTempFile(getClass().getSimpleName(), null);
    FileUtils.copyInputStreamToFile(getClass().getResourceAsStream("/converter/envelope.avro"), tmp);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tmp, datumReader);
    GenericRecord inputRecord = dataFileReader.next();

    Schema latestPayloadSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/record.avsc"));
    when(mockRegistry.getLatestSchemaByTopic(any())).thenReturn(latestPayloadSchema);
    when(mockRegistry.getSchemaByKey(any())).thenReturn(inputSchema.getField("nestedRecord").schema());

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(BaseEnvelopeSchemaConverter.PAYLOAD_SCHEMA_TOPIC, "test");
    workUnitState.setProp(BaseEnvelopeSchemaConverter.PAYLOAD_SCHEMA_ID_FIELD, "metadata.payloadSchemaId");
    workUnitState.setProp(BaseEnvelopeSchemaConverter.KAFKA_REGISTRY_FACTORY,
        EnvelopePayloadExtractingConverterTest.MockKafkaAvroSchemaRegistryFactory.class.getName());

    EnvelopePayloadExtractingConverter converter = new EnvelopePayloadExtractingConverter();
    converter.init(workUnitState);

    Schema outputSchema = converter.convertSchema(inputSchema, workUnitState);
    Assert.assertTrue(outputSchema.equals(latestPayloadSchema));

    List<GenericRecord> outputRecords = new ArrayList<>();
    Iterables.addAll(outputRecords, converter.convertRecord(outputSchema, inputRecord, workUnitState));
    Assert.assertTrue(outputRecords.size() == 1);

    GenericRecord payload = outputRecords.get(0);
    // While making the test envelope avro input record, its nestedRecord was intentionally set to the deserialized payload
    GenericRecord expectedPayload = (GenericRecord) inputRecord.get("nestedRecord");

    Schema payloadSchema = payload.getSchema();
    Schema expectedPayloadSchema = expectedPayload.getSchema();
    // The expected payload schema has the same number of fields as payload schema but in different order
    Assert.assertTrue(expectedPayloadSchema.getName().equals(payloadSchema.getName()));
    Assert.assertTrue(expectedPayloadSchema.getNamespace().equals(payloadSchema.getNamespace()));
    Assert.assertTrue(expectedPayloadSchema.getFields().size() == payloadSchema.getFields().size());

    for (Schema.Field field : payload.getSchema().getFields()) {
      Assert.assertTrue(expectedPayload.get(field.name()).equals(payload.get(field.name())));
    }
  }

  static class MockKafkaAvroSchemaRegistryFactory extends KafkaAvroSchemaRegistryFactory {
    @Override
    public KafkaSchemaRegistry create(Properties props) {
      return mockRegistry;
    }
  }
}
