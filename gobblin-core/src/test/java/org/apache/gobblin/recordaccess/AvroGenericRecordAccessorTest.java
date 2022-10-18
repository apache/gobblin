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
package org.apache.gobblin.recordaccess;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;


public class AvroGenericRecordAccessorTest {
  private Schema recordSchema;
  private GenericRecord record;
  private AvroGenericRecordAccessor accessor;

  @BeforeMethod
  public void initRecord() throws IOException {
    recordSchema =
        new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("converter/fieldPickInput.avsc"));
     record = new GenericData.Record(recordSchema);
    setRequiredRecordFields(record);

    accessor = new AvroGenericRecordAccessor(record);
  }

  @AfterMethod
  public void serializeRecord(ITestResult result)
      throws IOException {
    if (result.isSuccess() && result.getThrowable() == null) {
    /* Serialize the GenericRecord; this can catch issues in set() that the underlying GenericRecord
     * may not catch until serialize time
     */
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(recordSchema);
      ByteArrayOutputStream bOs = new ByteArrayOutputStream();

      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bOs, null);
      datumWriter.write(record, encoder);
      encoder.flush();
      bOs.flush();

      Assert.assertTrue(bOs.toByteArray().length > 0);
    }
  }

  @Test
  public void testSuccessfulSetAndGet() {
    accessor.set("name", "foo");
    accessor.set("favorite_number", 2);
    accessor.set("last_modified", 100L);

    Assert.assertEquals(accessor.getAsString("name"), "foo");
    Assert.assertEquals(accessor.getAsInt("favorite_number").intValue(), 2);
    Assert.assertEquals(accessor.getAsLong("last_modified").longValue(), 100L);
  }

  @Test
  public void testParsedRecordGet()
      throws IOException {
    updateRecordFromTestResource(ACCESSOR_RESOURCE_NAME);

    Assert.assertEquals(accessor.getAsString("name"), "testName");
    Assert.assertNull(accessor.getAsInt("favorite_number"));
    Assert.assertNull(accessor.getAsString("favorite_color"));
    Assert.assertEquals(accessor.getAsLong("last_modified").longValue(), 13L);
    Assert.assertEquals(accessor.getAsLong("created").longValue(), 14L);
  }

  @Test
  public void testParsedRecordManipulation()
      throws IOException {
    updateRecordFromTestResource(ACCESSOR_RESOURCE_NAME);

    accessor.set("name", "newName");

    Assert.assertEquals(accessor.getAsString("name"), "newName");
  }

  @Test
  public void testGetValueFromArray() throws IOException {
    setAccessorToRecordWithArrays();

    Assert.assertEquals(accessor.getAsString("nestedRecords.1.fieldToEncrypt"), "val1");
  }

  @Test
  public void testSetStringArray() throws IOException {
    List<String> quotes = ImmutableList.of("abracadabra", "hocuspocus");
    accessor.setStringArray("favorite_quotes", quotes);

    Assert.assertEquals(accessor.getGeneric("favorite_quotes"), quotes);
  }

  @Test
  public void testGetStringArrayUtf8() throws IOException {
    // Expectation: Even though we read an Avro object with UTF8 underneath, the accessor converts it into a
    // Java String
    List<String> expectedQuotes = ImmutableList.of("abc", "defg");

    GenericData.Array<Utf8> strings = new GenericData.Array<Utf8>(2, Schema.createArray(Schema.create(Schema.Type.STRING)));
    expectedQuotes.forEach(s -> strings.add(new Utf8(s)));
    record.put("favorite_quotes", strings);

    Assert.assertEquals(accessor.getGeneric("favorite_quotes"), expectedQuotes);
  }

  @Test
  public void testGetMultiConvertsStrings() throws IOException {
    // The below error is due to invalid avro data. As per avro, the default value must have the same type as the first 
    // entry in the union. As the default value is null, type with "null" union must have "null" type first and then
    // actual type. This is corrected in fieldPickInput.avsc file and fieldPickInput_arrays.avro
    // Error: org.apache.avro.AvroTypeException: Invalid default for field favorite_quotes: null
    // not a [{"type":"array","items":"string"},"null"]
    // Correct data: "type": ["null", { "type": "array", "items": "string"}, "default": null]
    updateRecordFromTestResource("converter/fieldPickInput", "converter/fieldPickInput_arrays.avro");
    Map<String, Object> ret = accessor.getMultiGeneric("favorite_quotes");
    Object val = ret.get("favorite_quotes");

    Assert.assertTrue(val instanceof List);
    List castedVal = (List)val;
    Assert.assertEquals(2, castedVal.size());
    Assert.assertEquals("hello world", castedVal.get(0));
    Assert.assertEquals("foobar", castedVal.get(1));
  }

  @Test
  public void testSetValueFromArray() throws IOException {
    setAccessorToRecordWithArrays();
    accessor.set("nestedRecords.1.fieldToEncrypt", "myNewVal");
    Assert.assertEquals(accessor.getAsString("nestedRecords.1.fieldToEncrypt"), "myNewVal");
  }

  @Test
  public void testGetMultiValue() throws IOException {
    setAccessorToRecordWithArrays();

    Map<String, String> fields = accessor.getMultiAsString("nestedRecords.*.fieldToEncrypt");
    Assert.assertEquals(fields.size(), 3);
    Assert.assertEquals(fields.get("nestedRecords.0.fieldToEncrypt"), "val0");
    Assert.assertEquals(fields.get("nestedRecords.1.fieldToEncrypt"), "val1");
    Assert.assertEquals(fields.get("nestedRecords.2.fieldToEncrypt"), "val2");
  }

  @Test(expectedExceptions = FieldDoesNotExistException.class)
  public void testSetNonexistentField() {
    accessor.set("doesnotexist", "someval");
  }

  @Test(expectedExceptions = FieldDoesNotExistException.class)
  public void testSetNonexistentNestedField() {
    accessor.set("subrecord.doesnotexist", "someval");
  }

  @Test(expectedExceptions = IncorrectTypeException.class)
  public void setBadTypePrimitive() {
    accessor.set("name", 5L);
  }

  @Test(expectedExceptions = IncorrectTypeException.class)
  public void setBadTypeUnion() {
    accessor.set("favorite_color", 0L);
  }

  @Test(expectedExceptions = IncorrectTypeException.class)
  public void getBadType() {
    accessor.getAsLong("name");
  }

  @Test
  public void testNestedSetAndGet()
      throws IOException {
    updateRecordFromTestResource(NESTED_RESOURCE_NAME);

    Assert.assertEquals(accessor.getAsString("address.city"), "Mountain view");
    accessor.set("address.city", "foobar");

    Assert.assertEquals(accessor.getAsString("address.city"), "foobar");
  }

  @Test
  public void setFieldToNull() {
    setRequiredRecordFields(record);

    accessor.setToNull("favorite_color");

    // afterTest serialization methods should ensure this works
  }

  private static void setRequiredRecordFields(GenericRecord record) {
    record.put("name", "validName");
    record.put("last_modified", 0L);
    record.put("favorite_number", 0);
    record.put("date_of_birth", 0L);
    record.put("created", 0L);
  }

  private void updateRecordFromTestResource(String resourceName) throws IOException {
    updateRecordFromTestResource(resourceName, null);
  }

  private void updateRecordFromTestResource(String resourceName, String avroFileName)
      throws IOException {
    if (avroFileName == null) {
      avroFileName = resourceName + ".avro";
    }

    recordSchema = new Schema.Parser().parse(
        getClass().getClassLoader().getResourceAsStream(resourceName + ".avsc")
    );

    DatumReader<GenericRecord> reader = new GenericDatumReader<>(recordSchema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
        new File(getClass().getClassLoader().getResource(avroFileName).getPath()), reader);

    Assert.assertTrue(dataFileReader.hasNext());
    record = dataFileReader.next(record);
    accessor = new AvroGenericRecordAccessor(record);
  }

  private void setAccessorToRecordWithArrays()
      throws IOException {
    updateRecordFromTestResource("converter/record_with_arrays");
  }

  private static final String ACCESSOR_RESOURCE_NAME = "converter/fieldPickInput";
  private static final String NESTED_RESOURCE_NAME = "converter/nested";
}
