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
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.test.TestUtils;


public class AvroToJsonStringConverterTest {
  private AvroToJsonStringConverter converter;
  private GenericRecord sampleRecord;
  private WorkUnitState state;

  @BeforeTest
  public void setUp() throws SchemaConversionException {
    sampleRecord = TestUtils.generateRandomAvroRecord();
    state = new WorkUnitState();

    converter = new AvroToJsonStringConverter();
    converter.convertSchema(sampleRecord.getSchema(), state);
  }

  @Test
  public void testRecord() throws DataConversionException, IOException {
    Iterable<String> records = converter.convertRecord(null, sampleRecord, state);
    Iterator<String> recordIt = records.iterator();
    ObjectMapper objectMapper = new ObjectMapper();

    String record = recordIt.next();

    Assert.assertFalse(recordIt.hasNext());
    JsonNode parsedRecord = objectMapper.readValue(record, JsonNode.class);

    Assert.assertEquals(parsedRecord.get("field1").getTextValue(), sampleRecord.get("field1").toString());
  }
}
