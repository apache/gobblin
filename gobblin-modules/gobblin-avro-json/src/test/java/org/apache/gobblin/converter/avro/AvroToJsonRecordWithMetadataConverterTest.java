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

import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.type.RecordWithMetadata;


public class AvroToJsonRecordWithMetadataConverterTest {
  private AvroToJsonRecordWithMetadataConverter converter;
  private WorkUnitState state;
  private GenericRecord sampleRecord;

  @BeforeTest
  public void setUp() throws SchemaConversionException {
    sampleRecord = TestUtils.generateRandomAvroRecord();
    state = new WorkUnitState();

    converter = new AvroToJsonRecordWithMetadataConverter();
    converter.convertSchema(sampleRecord.getSchema(), state);
  }

  @Test
  public void testRecord() throws DataConversionException {
    Iterable<RecordWithMetadata<JsonNode>> records = converter.convertRecord(null, sampleRecord, state);

    RecordWithMetadata<JsonNode> node = records.iterator().next();
    Assert.assertEquals(node.getMetadata().getGlobalMetadata().getContentType(), "test.name+json");
    Assert.assertEquals(node.getRecord().get("field1").getTextValue(), sampleRecord.get("field1").toString());
  }
}
