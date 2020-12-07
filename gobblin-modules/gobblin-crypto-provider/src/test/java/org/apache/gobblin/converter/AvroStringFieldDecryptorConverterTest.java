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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.test.crypto.InsecureShiftCodec;


public class AvroStringFieldDecryptorConverterTest {

  @Test
  public void testConversion()
      throws DataConversionException, IOException, SchemaConversionException {
    AvroStringFieldDecryptorConverter converter = new AvroStringFieldDecryptorConverter();
    WorkUnitState wuState = new WorkUnitState();

    wuState.getJobState().setProp("converter.fieldsToDecrypt", "field1");
    wuState.getJobState().setProp("converter.decrypt.AvroStringFieldDecryptorConverter.algorithm", "insecure_shift");

    converter.init(wuState);
    GenericRecord inputRecord = TestUtils.generateRandomAvroRecord();

    Schema inputSchema = inputRecord.getSchema();
    Schema outputSchema = converter.convertSchema(inputSchema, wuState);

    String fieldValue = (String) inputRecord.get("field1");

    Iterable<GenericRecord> recordIt = converter.convertRecord(outputSchema, inputRecord, wuState);
    GenericRecord decryptedRecord = recordIt.iterator().next();

    Assert.assertEquals(outputSchema, inputSchema);
    String decryptedValue = (String) decryptedRecord.get("field1");

    InsecureShiftCodec codec = new InsecureShiftCodec(Maps.<String, Object>newHashMap());
    InputStream in = codec.decodeInputStream(new ByteArrayInputStream(fieldValue.getBytes(StandardCharsets.UTF_8)));
    byte[] expectedDecryptedValue = new byte[in.available()];
    in.read(expectedDecryptedValue);

    Assert.assertEquals(new String(expectedDecryptedValue, StandardCharsets.UTF_8), decryptedValue);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testArrayDecryption()
      throws DataConversionException, IOException, SchemaConversionException {
    AvroStringFieldDecryptorConverter converter = new AvroStringFieldDecryptorConverter();
    WorkUnitState wuState = new WorkUnitState();

    wuState.getJobState().setProp("converter.fieldsToDecrypt", "array1");
    wuState.getJobState().setProp("converter.decrypt.AvroStringFieldDecryptorConverter.algorithm", "insecure_shift");

    converter.init(wuState);
    GenericRecord inputRecord = generateRecordWithArrays();

    Schema inputSchema = inputRecord.getSchema();
    Schema outputSchema = converter.convertSchema(inputSchema, wuState);

    GenericData.Array<String> fieldValue = (GenericData.Array<String>) inputRecord.get("array1");

    Iterable<GenericRecord> recordIt = converter.convertRecord(outputSchema, inputRecord, wuState);
    GenericRecord decryptedRecord = recordIt.iterator().next();

    Assert.assertEquals(outputSchema, inputSchema);
    GenericData.Array<String> decryptedValue = (GenericData.Array<String>) decryptedRecord.get("array1");

    for (int i = 0; i < decryptedValue.size(); i++) {
      assertDecryptedValuesEqual(decryptedValue.get(i), fieldValue.get(i));
    }
  }

  private void assertDecryptedValuesEqual(String decryptedValue, String originalValue) throws IOException {
    InsecureShiftCodec codec = new InsecureShiftCodec(Maps.<String, Object>newHashMap());
    InputStream in = codec.decodeInputStream(new ByteArrayInputStream(originalValue.getBytes(StandardCharsets.UTF_8)));
    byte[] expectedDecryptedValue = new byte[in.available()];
    in.read(expectedDecryptedValue);

    Assert.assertEquals(new String(expectedDecryptedValue, StandardCharsets.UTF_8), decryptedValue);
  }

  private GenericRecord getRecordFromFile(String path) throws IOException {
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(path), reader);
    while (dataFileReader.hasNext()) {
      return dataFileReader.next();
    }

    return null;
  }

  private GenericRecord generateRecordWithArrays() {
    ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
    String fieldName = "array1";
    Schema fieldSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    String docString = "doc";
    fields.add(new Schema.Field(fieldName, fieldSchema, docString, null));
    Schema schema = Schema.createRecord("name", docString, "test", false);
    schema.setFields(fields);

    GenericData.Record record = new GenericData.Record(schema);
    GenericData.Array<String> arr = new GenericData.Array<>(2, fieldSchema);
    arr.add("foobar");
    arr.add("foobaz");

    record.put("array1", arr);

    return record;
  }
}
