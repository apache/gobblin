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

package org.apache.gobblin.writer;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.orc.AvroOrcSchemaConverter;


public class OrcConverterMemoryManagerTest {

  @Test
  public void testBufferSizeCalculationResize()
      throws Exception {
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("list_map_test/schema.avsc"));
    TypeDescription orcSchema = AvroOrcSchemaConverter.getOrcSchema(schema);
    // Make batch size small so that the enlarge behavior would easily be triggered.
    VectorizedRowBatch rowBatch = orcSchema.createRowBatch(10);
    OrcConverterMemoryManager memoryManager = new OrcConverterMemoryManager(rowBatch, new State());
    GenericRecordToOrcValueWriter valueWriter = new GenericRecordToOrcValueWriter(orcSchema, schema);

    List<GenericRecord> recordList = GobblinOrcWriterTest
        .deserializeAvroRecords(this.getClass(), schema, "list_map_test/data.json");
    Assert.assertEquals(recordList.size(), 6);
    for (GenericRecord record : recordList) {
      valueWriter.write(record, rowBatch);
    }
    // Expected size is the size of the lists, map keys and map vals after resize. Since there are 6 records, and each array/map have at least 2 elements, then
    // One resize is performed when the respective list/maps exceed the initial size of 10, in this case 12.
    // So the resized total length would be 12*3 for the list, map keys and map vals, with 8 bytes per value .
    int expectedSize = 36 * 9 + 36 * 9 + 36 * 9 + 10*2;
    Assert.assertEquals(memoryManager.getConverterBufferTotalSize(), expectedSize);
  }

  @Test
  public void testBufferSizeCalculatedDeepNestedList() throws Exception {
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("converter_memory_manager_nested_test/schema.avsc"));
    TypeDescription orcSchema = AvroOrcSchemaConverter.getOrcSchema(schema);
    // Make batch such that only deeply nested list is resized
    VectorizedRowBatch rowBatch = orcSchema.createRowBatch(15);
    OrcConverterMemoryManager memoryManager = new OrcConverterMemoryManager(rowBatch, new State());
    GenericRecordToOrcValueWriter valueWriter = new GenericRecordToOrcValueWriter(orcSchema, schema);

    List<GenericRecord> recordList = GobblinOrcWriterTest
        .deserializeAvroRecords(this.getClass(), schema, "converter_memory_manager_nested_test/data.json");
    Assert.assertEquals(recordList.size(), 1);
    for (GenericRecord record : recordList) {
      valueWriter.write(record, rowBatch);
    }
    // Deeply nested list should be resized once, since it resizes at 30 elements (5+10+15) to 90
    // Other fields should not be resized, (map keys and vals, and top level arrays)
    // Account for size of top level arrays that should be small
    int expectedSize = 30*3*9 + 30*9 + 15*4; // Deeply nested list + maps + other structure overhead
    Assert.assertEquals(memoryManager.getConverterBufferTotalSize(), expectedSize);
  }

  @Test
  public void testBufferSmartResize() throws Exception {
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("converter_memory_manager_nested_test/schema.avsc"));
    TypeDescription orcSchema = AvroOrcSchemaConverter.getOrcSchema(schema);
    // Make batch such that only deeply nested list is resized
    VectorizedRowBatch rowBatch = orcSchema.createRowBatch(15);
    State memoryManagerState = new State();
    memoryManagerState.setProp(GobblinOrcWriterConfigs.ENABLE_SMART_ARRAY_ENLARGE, "true");
    memoryManagerState.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_DECAY_FACTOR, "0.5");
    memoryManagerState.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MAX, "10");
    memoryManagerState.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MIN, "1");
    OrcConverterMemoryManager memoryManager = new OrcConverterMemoryManager(rowBatch, memoryManagerState);

    int result = memoryManager.resize(1, 1000);
    Assert.assertEquals(result, 10000);

    // Result is equal to requested size since the decay factor dominates the resize
    result = memoryManager.resize(100, 1000);
    Assert.assertEquals(result, 1000);
  }

  @Test
  public void testBufferSmartResizeParameters() throws Exception {
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("converter_memory_manager_nested_test/schema.avsc"));
    TypeDescription orcSchema = AvroOrcSchemaConverter.getOrcSchema(schema);
    // Make batch such that only deeply nested list is resized
    VectorizedRowBatch rowBatch = orcSchema.createRowBatch(15);
    State memoryManagerState0 = new State();
    memoryManagerState0.setProp(GobblinOrcWriterConfigs.ENABLE_SMART_ARRAY_ENLARGE, "true");
    memoryManagerState0.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_DECAY_FACTOR, "0.5");
    memoryManagerState0.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MAX, "0");
    memoryManagerState0.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MIN, "1");
    Assert.assertThrows(IllegalArgumentException.class, () -> new OrcConverterMemoryManager(rowBatch, memoryManagerState0));

    State memoryManagerState1 = new State();
    memoryManagerState1.setProp(GobblinOrcWriterConfigs.ENABLE_SMART_ARRAY_ENLARGE, "true");
    memoryManagerState1.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_DECAY_FACTOR, "0.5");
    memoryManagerState1.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MAX, "1");
    memoryManagerState1.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MIN, "0");
    Assert.assertThrows(IllegalArgumentException.class, () -> new OrcConverterMemoryManager(rowBatch, memoryManagerState1));

    State memoryManagerState2 = new State();
    memoryManagerState2.setProp(GobblinOrcWriterConfigs.ENABLE_SMART_ARRAY_ENLARGE, "true");
    memoryManagerState2.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_DECAY_FACTOR, "1.5");
    memoryManagerState2.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MAX, "1");
    memoryManagerState2.setProp(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MIN, "1");
    Assert.assertThrows(IllegalArgumentException.class, () -> new OrcConverterMemoryManager(rowBatch, memoryManagerState2));
  }
}
