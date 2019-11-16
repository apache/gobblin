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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.util.AvroUtils;


public class AvroRecursionEliminatingConverterTest {


  public File generateRecord()
      throws IOException {
    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/recursive.avsc"));
    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(inputSchema);
    GenericRecord record = new GenericData.Record(inputSchema);
    record.put("name", "John");
    record.put("date_of_birth", 1234L);
    record.put("last_modified", 4567L);
    record.put("created", 6789L);
    GenericRecord addressRecord = new GenericData.Record(inputSchema.getField("address").schema());
    addressRecord.put("city", "Los Angeles");
    addressRecord.put("street_number", 1234);


    GenericRecord innerAddressRecord = new GenericData.Record(inputSchema.getField("address").schema());
    innerAddressRecord.put("city", "San Francisco");
    innerAddressRecord.put("street_number", 3456);

    addressRecord.put("previous_address", innerAddressRecord);
    record.put("address", addressRecord);

    File recordFile = File.createTempFile(this.getClass().getSimpleName(),"avsc");
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(inputSchema, recordFile);
    dataFileWriter.append(record);
    dataFileWriter.close();
    recordFile.deleteOnExit();
    return recordFile;
  }

  /**
   * Test schema and record conversion using a recursive schema
   */
  @Test
  public void testConversion()
      throws IOException {

    File inputFile = generateRecord();

    WorkUnitState workUnitState = new WorkUnitState();

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/recursive.avsc"));
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(inputSchema);

    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(inputFile, datumReader);
    GenericRecord inputRecord = dataFileReader.next();

    AvroRecursionEliminatingConverter converter = new AvroRecursionEliminatingConverter();
    Schema outputSchema = null;
    String recursiveFieldPath = "address.previous_address";
    // test that the inner recursive field is present in input schema
    Assert.assertTrue(AvroUtils.getFieldSchema(inputSchema,  recursiveFieldPath).isPresent());
    try {
      outputSchema = converter.convertSchema(inputSchema, workUnitState);
      // test that the inner recursive field is no longer in the schema
      Assert.assertTrue(!AvroUtils.getFieldSchema(outputSchema, recursiveFieldPath).isPresent(),
          "Inner recursive field " + recursiveFieldPath + " should not be in output schema");
    } catch (SchemaConversionException e) {
      Assert.fail(e.getMessage());
    }

    GenericRecord outputRecord = null;
    try {
      outputRecord = converter.convertRecord(outputSchema, inputRecord, workUnitState).iterator().next();
    } catch (DataConversionException e) {
      Assert.fail(e.getMessage());
    }

    checkEquality("address.street_number", inputRecord, 1234, "Different value in input");
    checkEquality("address.street_number", outputRecord, 1234, "Different value in output");
    checkEquality("name", inputRecord, new Utf8("John"), "Different value in input");
    checkEquality("name", outputRecord, new Utf8("John"), "Different value in output");

    // check that inner address record exists in input record
    checkEquality("address.previous_address.city", inputRecord, new Utf8("San Francisco"), "Different value in input");
    checkEquality("address.previous_address", outputRecord, null, "Failed to remove recursive field");

  }

  private void checkEquality(String fieldPath, GenericRecord inputRecord, Object expected, String message) {
    Optional inputValue = AvroUtils.getFieldValue(inputRecord, fieldPath);
    if (expected != null) {
      Assert.assertEquals(inputValue.get(), expected, message);
    } else {
      Assert.assertTrue(!inputValue.isPresent(), message);
    }

  }

}
