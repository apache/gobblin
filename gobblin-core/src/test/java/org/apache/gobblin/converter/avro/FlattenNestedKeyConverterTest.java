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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import junit.framework.Assert;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.util.AvroUtils;


@Test
public class FlattenNestedKeyConverterTest {
  /**
   * Test schema and record conversion
   *  1. A successful schema and record conversion
   *  2. Another successful conversion by reusing the converter
   *  3. An expected failed conversion by reusing the converter
   */
  public void testConversion()
      throws IOException {
    String key = FlattenNestedKeyConverter.class.getSimpleName() + "." + FlattenNestedKeyConverter.FIELDS_TO_FLATTEN;
    Properties props = new Properties();
    props.put(key, "name,address.street_number");
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.addAll(props);

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/nested.avsc"));
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(inputSchema);

    File tmp = File.createTempFile(this.getClass().getSimpleName(), null);
    FileUtils.copyInputStreamToFile(getClass().getResourceAsStream("/converter/nested.avro"), tmp);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(tmp, datumReader);
    GenericRecord inputRecord = dataFileReader.next();

    FlattenNestedKeyConverter converter = new FlattenNestedKeyConverter();
    Schema outputSchema = null;
    try {
      outputSchema = converter.convertSchema(inputSchema, workUnitState);
    } catch (SchemaConversionException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertTrue(outputSchema.getFields().size() == inputSchema.getFields().size() + 1);
    Assert.assertTrue(outputSchema.getField("addressStreet_number") != null);

    GenericRecord outputRecord = null;
    try {
      outputRecord = converter.convertRecord(outputSchema, inputRecord, workUnitState).iterator().next();
    } catch (DataConversionException e) {
      Assert.fail(e.getMessage());
    }
    Object expected = AvroUtils.getFieldValue(outputRecord, "address.street_number").get();
    Assert.assertTrue(outputRecord.get("addressStreet_number") == expected);

    // Reuse the converter to do another successful conversion
    props.put(key, "name,address.city");
    workUnitState.addAll(props);
    try {
      outputSchema = converter.convertSchema(inputSchema, workUnitState);
    } catch (SchemaConversionException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertTrue(outputSchema.getFields().size() == inputSchema.getFields().size() + 1);
    Assert.assertTrue(outputSchema.getField("addressCity") != null);
    try {
      outputRecord = converter.convertRecord(outputSchema, inputRecord, workUnitState).iterator().next();
    } catch (DataConversionException e) {
      Assert.fail(e.getMessage());
    }
    expected = AvroUtils.getFieldValue(outputRecord, "address.city").get();
    Assert.assertTrue(outputRecord.get("addressCity") == expected);

    // Reuse the converter to do a failed conversion
    props.put(key, "name,address.anInvalidField");
    workUnitState.addAll(props);
    boolean hasAnException = false;
    try {
      converter.convertSchema(inputSchema, workUnitState);
    } catch (SchemaConversionException e) {
      hasAnException = true;
    }
    Assert.assertTrue(hasAnException);
  }
}
