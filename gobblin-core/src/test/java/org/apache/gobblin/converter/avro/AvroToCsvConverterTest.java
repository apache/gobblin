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
package org.apache.gobblin.converter.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


public class AvroToCsvConverterTest {
  @Test
  public void testConversion()
      throws DataConversionException, IOException, SchemaConversionException {
    Schema inputSchema = new Schema.Parser()
        .parse(getClass().getClassLoader().getResourceAsStream("converter/bytes_to_avro/test_record_schema.avsc"));

    AvroToCsvConverter converter = new AvroToCsvConverter();
    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_TO_CSV_IS_HEADER_INCLUDED, true);
    converter.init(state);
    String outputSchema = converter.convertSchema(inputSchema, state);

    GenericRecord testRecord1 = new GenericData.Record(inputSchema);
    testRecord1.put("testStr", "testing12");
    testRecord1.put("testInt", -2);

    // First record should come back with Header
    List<String> records = Lists.newArrayList(converter.convertRecord(outputSchema, testRecord1, state).iterator());

    Assert.assertEquals(records.size(), 2, "Expected size with header is 2 records");
    Assert.assertEquals(records.get(0), "testStr,testInt", "Header did not match.");
    Assert.assertEquals(records.get(1), "testing12,-2", "Converted record did not match.");

    GenericRecord testRecord2 = new GenericData.Record(inputSchema);
    testRecord2.put("testStr", "testing23");
    testRecord2.put("testInt", 2);

    // Second record should come back without Header
    records = Lists.newArrayList(converter.convertRecord(outputSchema, testRecord2, state).iterator());

    Assert.assertEquals(records.size(), 1, "Expected size with header is 1 records");
    Assert.assertEquals(records.get(0), "testing23,2", "Converted record did not match.");
  }
}
