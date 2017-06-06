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
package gobblin.converter.avro;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


public class AvroToBytesConverterTest {
  @Test
  public void testSerialization()
      throws DataConversionException, IOException, SchemaConversionException {
    Schema inputSchema = new Schema.Parser()
        .parse(getClass().getClassLoader().getResourceAsStream("converter/bytes_to_avro/test_record_schema.avsc"));

    AvroToBytesConverter converter = new AvroToBytesConverter();
    WorkUnitState state = new WorkUnitState();
    converter.init(state);
    String outputSchema = converter.convertSchema(inputSchema, state);

    // Write a record twice to make sure nothing goes wrong with caching
    for (int i = 0; i < 2; i++) {
      GenericRecord testRecord = new GenericData.Record(inputSchema);
      testRecord.put("testStr", "testing12" + ((i == 0) ? "3": "4"));
      testRecord.put("testInt", -2);

      Iterator<byte[]> records = converter.convertRecord(outputSchema, testRecord, state).iterator();
      byte[] record = records.next();

      Assert.assertFalse(records.hasNext());
      byte[] expectedRecord = IOUtils.toByteArray(getClass().getClassLoader().getResourceAsStream("converter/bytes_to_avro/test_record_binary.avro"));

      // the serialized record was serialized with testing123 as the string; if we write testing124 out
      // contents should be the same except for the 10th byte which will be '4' instead of '3'
      if (i == 1) {
        expectedRecord[10] = 52;
      }

      Assert.assertEquals(outputSchema, inputSchema.toString());
      Assert.assertEquals(record, expectedRecord);
    }
  }
}
