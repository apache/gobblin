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
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.test.crypto.InsecureShiftCodec;


public class AvroStringFieldEncryptorConverterTest {
  @Test
  public void testNestedConversion()
      throws DataConversionException, IOException, SchemaConversionException {
    AvroStringFieldEncryptorConverter converter = new AvroStringFieldEncryptorConverter();
    WorkUnitState wuState = new WorkUnitState();

    wuState.getJobState().setProp("converter.fieldsToEncrypt", "nestedRecords.*.fieldToEncrypt");
    wuState.getJobState().setProp("converter.encrypt.algorithm", "insecure_shift");

    converter.init(wuState);
    GenericRecord inputRecord =
        getRecordFromFile(getClass().getClassLoader().getResource("record_with_arrays.avro").getPath());

    Schema inputSchema = inputRecord.getSchema();
    Schema outputSchema = converter.convertSchema(inputSchema, wuState);

    List<String> origValues = new ArrayList<>();
    for (Object o : (List) inputRecord.get("nestedRecords")) {
      GenericRecord r = (GenericRecord) o;
      origValues.add(r.get("fieldToEncrypt").toString());
    }

    Iterable<GenericRecord> recordIt = converter.convertRecord(outputSchema, inputRecord, wuState);
    GenericRecord record = recordIt.iterator().next();

    Assert.assertEquals(outputSchema, inputSchema);

    List<String> decryptedValues = new ArrayList<>();
    for (Object o : (List) record.get("nestedRecords")) {
      GenericRecord r = (GenericRecord) o;
      String encryptedValue = r.get("fieldToEncrypt").toString();

      InsecureShiftCodec codec = new InsecureShiftCodec(Maps.<String, Object>newHashMap());
      InputStream in =
          codec.decodeInputStream(new ByteArrayInputStream(encryptedValue.getBytes(StandardCharsets.UTF_8)));
      byte[] decryptedValue = new byte[in.available()];
      in.read(decryptedValue);
      decryptedValues.add(new String(decryptedValue, StandardCharsets.UTF_8));
    }

    Assert.assertEquals(decryptedValues, origValues);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEncryptionOfArray()
      throws SchemaConversionException, DataConversionException, IOException {
    AvroStringFieldEncryptorConverter converter = new AvroStringFieldEncryptorConverter();
    WorkUnitState wuState = new WorkUnitState();

    wuState.getJobState().setProp("converter.fieldsToEncrypt", "favorite_quotes");
    wuState.getJobState().setProp("converter.encrypt.algorithm", "insecure_shift");

    converter.init(wuState);
    // The below error is due to invalid avro data. As per avro, the default value must have the same type as the first 
    // entry in the union. As the default value is null, type with "null" union must have "null" type first and then
    // actual type. This is corrected in fieldPickInput.avsc file and fieldPickInput_arrays.avro
    // Error: org.apache.avro.AvroTypeException: Invalid default for field favorite_quotes: null
    // not a [{"type":"array","items":"string"},"null"]
    // Correct data: "type": ["null", { "type": "array", "items": "string"}, "default": null]
    GenericRecord inputRecord =
        getRecordFromFile(getClass().getClassLoader().getResource("fieldPickInput_arrays.avro").getPath());
    GenericArray origValues = (GenericArray) inputRecord.get("favorite_quotes");
    for (int i = 0; i < origValues.size(); i++) {
      origValues.set(i, origValues.get(i).toString());
    }

    Schema inputSchema = inputRecord.getSchema();
    Schema outputSchema = converter.convertSchema(inputSchema, wuState);

    Iterable<GenericRecord> recordIt = converter.convertRecord(outputSchema, inputRecord, wuState);
    GenericRecord encryptedRecord = recordIt.iterator().next();

    Assert.assertEquals(outputSchema, inputSchema);

    GenericArray<String> encryptedVals = (GenericArray<String>) encryptedRecord.get("favorite_quotes");
    List<String> decryptedValues = Lists.newArrayList();
    for (String encryptedValue: encryptedVals) {
      InsecureShiftCodec codec = new InsecureShiftCodec(Maps.<String, Object>newHashMap());
      InputStream in =
          codec.decodeInputStream(new ByteArrayInputStream(encryptedValue.getBytes(StandardCharsets.UTF_8)));
      byte[] decryptedValue = new byte[in.available()];
      in.read(decryptedValue);
      decryptedValues.add(new String(decryptedValue, StandardCharsets.UTF_8));
    }

    Assert.assertEquals(decryptedValues, origValues);
  }

  private GenericArray<String> buildTestArray() {
    Schema s = Schema.createArray(Schema.create(Schema.Type.STRING));
    GenericArray<String> arr = new GenericData.Array<>(3, s);
    arr.add("one");
    arr.add("two");
    arr.add("three");

    return arr;
  }

  private GenericRecord getRecordFromFile(String path)
      throws IOException {
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(path), reader);
    if (dataFileReader.hasNext()) {
      return dataFileReader.next();
    }

    return null;
  }

  @Test
  public void testConversion()
      throws DataConversionException, IOException, SchemaConversionException {
    AvroStringFieldEncryptorConverter converter = new AvroStringFieldEncryptorConverter();
    WorkUnitState wuState = new WorkUnitState();

    wuState.getJobState().setProp("converter.fieldsToEncrypt", "field1");
    wuState.getJobState().setProp("converter.encrypt.algorithm", "insecure_shift");

    converter.init(wuState);
    GenericRecord inputRecord = TestUtils.generateRandomAvroRecord();

    Schema inputSchema = inputRecord.getSchema();
    Schema outputSchema = converter.convertSchema(inputSchema, wuState);

    String fieldValue = (String) inputRecord.get("field1");

    Iterable<GenericRecord> recordIt = converter.convertRecord(outputSchema, inputRecord, wuState);
    GenericRecord record = recordIt.iterator().next();

    Assert.assertEquals(outputSchema, inputSchema);
    String encryptedValue = (String) record.get("field1");

    InsecureShiftCodec codec = new InsecureShiftCodec(Maps.<String, Object>newHashMap());
    InputStream in = codec.decodeInputStream(new ByteArrayInputStream(encryptedValue.getBytes(StandardCharsets.UTF_8)));
    byte[] decryptedValue = new byte[in.available()];
    in.read(decryptedValue);

    Assert.assertEquals(new String(decryptedValue, StandardCharsets.UTF_8), fieldValue);
  }
}
