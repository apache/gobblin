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

package org.apache.gobblin.converter.filter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.util.test.TestIOUtils;


public class AvroProjectionConverterTest {

  @Test
  public void testRemoveWithNamespace()
      throws Exception {

    WorkUnitState wus = new WorkUnitState();
    wus.setProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "db1");
    wus.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "table1");
    wus.setProp(AvroProjectionConverter.USE_NAMESPACE, true);

    GenericRecord inputRecord = TestIOUtils.readAllRecords(
        getClass().getResource("/avroProjectionConverter/simpleRecord.json").getPath(),
        getClass().getResource("/avroProjectionConverter/simpleRecord.avsc").getPath()).get(0);
    Schema inputSchema = inputRecord.getSchema();

    AvroProjectionConverter converter = new AvroProjectionConverter();
    // Test no field removed with table1.remove.fields
    wus.setProp("table1.remove.fields", "id");
    converter.init(wus);
    Schema outputSchema = converter.convertSchema(inputSchema, wus);
    Assert.assertEquals(outputSchema.getFields().size(), 2);

    // Field successfully removed
    wus.setProp("db1.table1.remove.fields", "id");
    converter.init(wus);
    outputSchema = converter.convertSchema(inputSchema, wus);
    Assert.assertEquals(outputSchema.getFields().size(), 1);

    GenericRecord outputRecord = converter.convertRecord(outputSchema, inputRecord, wus).iterator().next();
    Assert.assertEquals(outputRecord.toString(), "{\"created\": 20170906185911}");
  }
}
