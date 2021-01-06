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

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamTestUtils;
import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;

public class KafkaSchemaChangeInjectorTest {
  // Test Avro schemas
  private static final String SCHEMA1 = "{\"namespace\": \"example.avro\",\n" +
      " \"type\": \"record\",\n" +
      " \"name\": \"user\",\n" +
      " \"fields\": [\n" +
      "     {\"name\": \"name\", \"type\": \"string\"},\n" +
      "     {\"name\": \"DUMMY\", \"type\": [\"null\",\"string\"]}\n" +
      " ]\n" +
      "}";

  @Test
  public void testInjection() throws SchemaRegistryException, IOException {

    String datasetName = "topic1";

    class TestInjector extends KafkaSchemaChangeInjector<Schema> {

      @Override
      protected Schema getSchemaIdentifier(DecodeableKafkaRecord consumerRecord) {
        return ((GenericRecord) consumerRecord.getValue()).getSchema();
      }
    }

    KafkaSchemaChangeInjector schemaChangeInjector = new TestInjector();
    WorkUnitState wus = new WorkUnitState();
    wus.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, KafkaStreamTestUtils.MockSchemaRegistry.class.getName());
    schemaChangeInjector.init(wus);

    Schema schema1 = new Schema.Parser().parse(SCHEMA1);
    Schema schema2 = new Schema.Parser().parse(SCHEMA1.replace("DUMMY", "DUMMY2"));
    Schema schema3 = new Schema.Parser().parse(SCHEMA1.replace("DUMMY", "DUMMY3"));
    Schema schema4 = new Schema.Parser().parse(SCHEMA1.replace("DUMMY", "DUMMY4"));

    schemaChangeInjector.setInputGlobalMetadata(GlobalMetadata.<Schema>builder().schema(schema1).build(), null);

    schemaChangeInjector.getSchemaRegistry().register(schema1, datasetName);

    DecodeableKafkaRecord record1 = getMock(datasetName, getRecord(schema1, "name1"));
    DecodeableKafkaRecord record2 = getMock(datasetName, getRecord(schema2, "name1"));
    DecodeableKafkaRecord record3 = getMock(datasetName, getRecord(schema3, "name1"));
    DecodeableKafkaRecord record4 = getMock(datasetName, getRecord(schema4, "name1"));

    // first message will always trigger injection
    Assert.assertEquals(schemaChangeInjector.getSchemaCache().size(), 0);
    Assert.assertNotNull(schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<>(record1), wus));

    // next messages should not trigger injection since there is no schema change
    Assert.assertNull(schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<>(record1), wus));
    Assert.assertNull(schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<>(record1), wus));
    Assert.assertEquals(schemaChangeInjector.getSchemaCache().size(), 1);

    Assert.assertNull(schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<>(record2), wus));
    Assert.assertEquals(schemaChangeInjector.getSchemaCache().size(), 2);

    // updating the latest schema should result in an injection
    schemaChangeInjector.getSchemaRegistry().register(schema4, datasetName);

    Iterable<ControlMessage<DecodeableKafkaRecord>> iterable =
        schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<>(record3), wus);

    Assert.assertNotNull(iterable);

    List<ControlMessage<DecodeableKafkaRecord>> controlMessages = Lists.newArrayList(iterable);

    Assert.assertEquals(controlMessages.size(), 1);

    // Should not see any injections since no schema update after the last call
    Assert.assertNull(schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<>(record4), wus));
  }

  private DecodeableKafkaRecord getMock(String datasetName, GenericRecord record) {
    DecodeableKafkaRecord mockRecord = Mockito.mock(DecodeableKafkaRecord.class);
    Mockito.when(mockRecord.getValue()).thenReturn(record);
    Mockito.when(mockRecord.getTopic()).thenReturn(datasetName);
    Mockito.when(mockRecord.getPartition()).thenReturn(1);
    return mockRecord;
  }

  private GenericRecord getRecord(Schema schema, String name) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("name", name);

    return record;
  }
}