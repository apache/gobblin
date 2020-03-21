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

package org.apache.gobblin.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AvroUtilsTest {
  private static final String AVRO_DIR = "gobblin-utility/src/test/resources/avroDirParent/";

  @Test
  public void testSchemaCompatiability() {
    Schema readerSchema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"GobblinTrackingEvent_GaaS2\",\"namespace\":\"gobblin.metrics\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Time at which event was created.\",\"default\":0},{\"name\":\"namespace\",\"type\":[{\"type\":\"string\",\"avro.java.string\":\"String\"},\"null\"],\"doc\":\"Namespace used for filtering of events.\"},{\"name\":\"name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Event name.\"},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"avro.java.string\":\"String\"},\"doc\":\"Event metadata.\",\"default\":{}}]}");
    Schema writerSchema1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"GobblinTrackingEvent\",\"namespace\":\"org.apache.gobblin.metrics\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Time at which event was created.\",\"default\":0},{\"name\":\"namespace\",\"type\":[\"string\",\"null\"],\"doc\":\"Namespace used for filtering of events.\"},{\"name\":\"name\",\"type\":\"string\",\"doc\":\"Event name.\"},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"doc\":\"Event metadata.\",\"default\":{}}]}");
    Schema writerSchema2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"GobblinTrackingEvent\",\"namespace\":\"org.apache.gobblin.metrics\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Time at which event was created.\",\"default\":0},{\"name\":\"namespace\",\"type\":[\"string\",\"null\"],\"doc\":\"Namespace used for filtering of events.\"},{\"name\":\"name2\",\"type\":\"string\",\"doc\":\"Event name.\"},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"doc\":\"Event metadata.\",\"default\":{}}]}");

    Assert.assertTrue(AvroUtils.checkReaderWriterCompatibility(readerSchema, writerSchema1, true));
    Assert.assertFalse(AvroUtils.checkReaderWriterCompatibility(readerSchema, writerSchema1, false));
    Assert.assertFalse(AvroUtils.checkReaderWriterCompatibility(readerSchema, writerSchema2, true));
  }

  @Test
  public void testGetDirectorySchema() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    Path mockAvroFilePath = new Path(AVRO_DIR);
    Assert.assertNotNull(AvroUtils.getDirectorySchema(mockAvroFilePath, conf, true));
  }

  /**
   * Test nullifying fields for non-union types, including array.
   */
  @Test
  public void testNullifyFieldForNonUnionSchemaMerge() {
    Schema oldSchema1 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, " + "{\"name\": \"number\", \"type\": \"int\"}" + "]}");

    Schema newSchema1 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema1 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, " + "{\"name\": \"number\", \"type\": [\"null\", \"int\"]}"
            + "]}");

    Assert.assertEquals(expectedOutputSchema1, AvroUtils.nullifyFieldsForSchemaMerge(oldSchema1, newSchema1));

    Schema oldSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"number\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}" + "]}");

    Schema newSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"number\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}" + "]}");

    Assert.assertEquals(expectedOutputSchema2, AvroUtils.nullifyFieldsForSchemaMerge(oldSchema2, newSchema2));
  }

  /**
   * Test nullifying fields for union type. One case does not have "null", and the other case already has a default "null".
   */
  @Test
  public void testNullifyFieldForUnionSchemaMerge() {

    Schema oldSchema1 =
        new Schema.Parser()
            .parse("{\"type\":\"record\", \"name\":\"test\", "
                + "\"fields\":["
                + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"number\", \"type\": [{\"type\": \"string\"}, {\"type\": \"array\", \"items\": \"string\"}]}"
                + "]}");

    Schema newSchema1 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema1 =
        new Schema.Parser()
            .parse("{\"type\":\"record\", \"name\":\"test\", "
                + "\"fields\":["
                + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"number\", \"type\": [\"null\", {\"type\": \"string\"}, {\"type\": \"array\", \"items\": \"string\"}]}"
                + "]}");

    Assert.assertEquals(expectedOutputSchema1, AvroUtils.nullifyFieldsForSchemaMerge(oldSchema1, newSchema1));

    Schema oldSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"number\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}" + "]}");

    Schema newSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema2 =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"number\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}" + "]}");

    Assert.assertEquals(expectedOutputSchema2, AvroUtils.nullifyFieldsForSchemaMerge(oldSchema2, newSchema2));
  }

  /**
   * Test nullifying fields when more than one field is removed in the new schema.
   */
  @Test
  public void testNullifyFieldForMultipleFieldsRemoved() {

    Schema oldSchema =
        new Schema.Parser()
            .parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
                + "{\"name\": \"name\", \"type\": \"string\"}, " + "{\"name\": \"color\", \"type\": \"string\"}, "
                + "{\"name\": \"number\", \"type\": [{\"type\": \"string\"}, {\"type\": \"array\", \"items\": \"string\"}]}"
                + "]}");

    Schema newSchema =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema =
        new Schema.Parser()
            .parse("{\"type\":\"record\", \"name\":\"test\", "
                + "\"fields\":["
                + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"color\", \"type\": [\"null\", \"string\"]}, "
                + "{\"name\": \"number\", \"type\": [\"null\", {\"type\": \"string\"}, {\"type\": \"array\", \"items\": \"string\"}]}"
                + "]}");
    Assert.assertEquals(expectedOutputSchema, AvroUtils.nullifyFieldsForSchemaMerge(oldSchema, newSchema));
  }

  /**
   * Test nullifying fields when one schema is not record type.
   */
  @Test
  public void testNullifyFieldWhenOldSchemaNotRecord() {

    Schema oldSchema = new Schema.Parser().parse("{\"type\": \"array\", \"items\": \"string\"}");

    Schema newSchema =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}" + "]}");

    Schema expectedOutputSchema = newSchema;
    Assert.assertEquals(expectedOutputSchema, AvroUtils.nullifyFieldsForSchemaMerge(oldSchema, newSchema));
  }

  @Test
  public void testSwitchName() {
    String originalName = "originalName";
    String newName = "newName";
    Schema schema = SchemaBuilder.record(originalName).fields().
        requiredDouble("double").optionalFloat("float").endRecord();

    Schema newSchema = AvroUtils.switchName(schema, newName);

    Assert.assertEquals(newSchema.getName(), newName);
    for(Schema.Field field : newSchema.getFields()) {
      Assert.assertEquals(field, schema.getField(field.name()));
    }

    Assert.assertEquals(newName, AvroUtils.switchName(schema, newName).getName());
    Assert.assertEquals(schema,
        AvroUtils.switchName(AvroUtils.switchName(schema, newName), schema.getName()));

  }

  @Test
  public void testSwitchNamespace() {
    String originalNamespace = "originalNamespace";
    String originalName = "originalName";
    String newNamespace = "newNamespace";
    Schema schema = SchemaBuilder.builder(originalNamespace).record(originalName).fields().
        requiredDouble("double").optionalFloat("float").endRecord();

    Map<String, String> map = Maps.newHashMap();
    map.put(originalNamespace, newNamespace);
    Schema newSchema = AvroUtils.switchNamespace(schema, map);

    Assert.assertEquals(newSchema.getNamespace(), newNamespace);
    Assert.assertEquals(newSchema.getName(), originalName);
    for(Schema.Field field : newSchema.getFields()) {
      Assert.assertEquals(field, schema.getField(field.name()));
    }
  }

  @Test public void testSerializeAsPath() throws Exception {

    Schema schema =
        new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"test\", " + "\"fields\":["
            + "{\"name\": \"name\", \"type\": \"string\"}, " + "{\"name\": \"title\", \"type\": \"string\"}" + "]}");

    GenericRecord partition = new GenericData.Record(schema);
    partition.put("name", "a/b:c\\d e");
    partition.put("title", "title");

    Assert.assertEquals(AvroUtils.serializeAsPath(partition, true, true), new Path("name=a_b_c_d_e/title=title"));
    Assert.assertEquals(AvroUtils.serializeAsPath(partition, false, true), new Path("a_b_c_d_e/title"));
    Assert.assertEquals(AvroUtils.serializeAsPath(partition, false, false), new Path("a/b_c_d_e/title"));
  }

  @Test
  public void testStringEscaping() {
    String invalidString = "foo;foo'bar";
    String expectedString = "foo\\;foo\\'bar";
    String actualString = AvroUtils.sanitizeSchemaString(invalidString);
    Assert.assertEquals(actualString, expectedString);
    // Verify that there's only one slash being added.
    Assert.assertEquals(actualString.length(), invalidString.length() + 2);
  }

  public static List<GenericRecord> getRecordFromFile(String path)
      throws IOException {
    Configuration config = new Configuration();
    SeekableInput input = new FsInput(new Path(path), config);
    DatumReader<GenericRecord> reader1 = new GenericDatumReader<>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader1);
    List<GenericRecord> records = new ArrayList<>();
    for (GenericRecord datum : fileReader) {
      records.add(datum);
    }
    fileReader.close();
    return records;
  }

  /**
   * This is a test to validate support of maps in {@link org.apache.gobblin.util.AvroUtils#getFieldValue(GenericRecord, String)}
   * and {@link org.apache.gobblin.util.AvroUtils#getFieldSchema(Schema, String)}
   * @throws IOException
   */

  @Test
  public void testGetObjectFromMap()
      throws IOException {
    final String TEST_FIELD_LOCATION = "Map.stringKey.Field";
    String avroFilePath = this.AVRO_DIR + "avroDir/avroUtilsTestFile.avro";
    GenericRecord record = getRecordFromFile(avroFilePath).get(0);
    Assert.assertEquals(AvroUtils.getFieldValue(record, TEST_FIELD_LOCATION).get().toString(), "stringValue2");
    Assert.assertEquals(AvroUtils.getFieldSchema(record.getSchema(), TEST_FIELD_LOCATION).get().getType(),
        Schema.Type.STRING);
  }

  /**
   * In case of complex data types in union {@link AvroUtils#getFieldSchema(Schema, String)} should throw {@link AvroRuntimeException}
   * @throws IOException
   */

  @Test(expectedExceptions = AvroRuntimeException.class)
  public void testComplexTypesInUnionNotSupported()
      throws IOException {
    final String TEST_LOCATION = "TestUnionObject.RecordInUnion";
    String avroFilePath = this.AVRO_DIR + "avroDir/avroUtilsTestFile.avro";
    GenericRecord record = getRecordFromFile(avroFilePath).get(0);

    AvroUtils.getFieldSchema(record.getSchema(), TEST_LOCATION);
  }

  @Test
  public void testUnionWithNull() {
    Schema nestedRecord = SchemaBuilder.record("nested").fields().requiredDouble("double")
        .requiredString("string").endRecord();
    Schema union = SchemaBuilder.unionOf().nullType().and().type(nestedRecord).endUnion();
    Schema schema = SchemaBuilder.record("record").fields().name("union").type(union).noDefault().endRecord();

    Schema doubleSchema = AvroUtils.getFieldSchema(schema, "union.double").get();
    Assert.assertEquals(doubleSchema.getType(), Schema.Type.DOUBLE);

    GenericRecord nested = new GenericData.Record(nestedRecord);
    nested.put("double", 10);
    nested.put("string", "testString");
    GenericRecord record = new GenericData.Record(schema);
    record.put("union", nested);

    String stringValue = AvroUtils.getFieldValue(record, "union.string").get().toString();
    Assert.assertEquals(stringValue, "testString");
  }


  @Test
  public void testDecorateSchemaWithSingleField() {

    Schema inputRecord = SchemaBuilder.record("test").fields().requiredInt("numeric1")
            .requiredString("string1").endRecord();
    Schema fieldSchema = SchemaBuilder.builder().intType();
    Schema.Field field = new Schema.Field("newField", fieldSchema, "",null);
    Schema outputRecord = AvroUtils.decorateRecordSchema(inputRecord, Collections.singletonList(field));
    checkFieldsMatch(inputRecord, outputRecord);
    Assert.assertNotNull(outputRecord.getField("newField"));
    Assert.assertEquals(outputRecord.getField("newField").schema(), fieldSchema);
  }

  private void checkFieldsMatch(Schema inputRecord, Schema outputRecord) {
    inputRecord.getFields().forEach(f -> {
      Schema.Field outField = outputRecord.getField(f.name());
      Assert.assertEquals(f, outField);
    });
  }

  @Test
  public void testDecorateSchemaWithStringProperties() {

    Schema inputRecord = SchemaBuilder.record("test").fields()
            .name("integer1")
              .prop("innerProp", "innerVal")
              .type().intBuilder().endInt().noDefault()
            .requiredString("string1")
            .endRecord();
    inputRecord.addProp("topLevelProp", "topLevelVal");
    Schema.Field additionalField = getTestInnerRecordField();
    Schema outputSchema = AvroUtils.decorateRecordSchema(inputRecord, Collections.singletonList(additionalField));
    checkFieldsMatch(inputRecord, outputSchema);
    Assert.assertEquals(outputSchema.getProp("topLevelProp"), "topLevelVal");
    Assert.assertEquals(outputSchema.getField("integer1").getProp("innerProp"), "innerVal");
  }

  @Test
  public void testDecorateSchemaWithObjectProperties() throws IOException {

    String customPropertyString = "{\"custom\": {\"prop1\": \"val1\"}}";
    JsonNode customPropertyValue = new ObjectMapper().readTree(customPropertyString);

    Schema inputRecord = SchemaBuilder.record("test").fields()
            .name("integer1")
            .prop("innerProp", "innerVal")
            .type().intBuilder().endInt().noDefault()
            .requiredString("string1")
            .endRecord();
    inputRecord.addProp("topLevelProp", customPropertyValue);
    Schema.Field additionalField = getTestInnerRecordField();
    Schema outputSchema = AvroUtils.decorateRecordSchema(inputRecord, Collections.singletonList(additionalField));
    checkFieldsMatch(inputRecord, outputSchema);
    Assert.assertEquals(outputSchema.getProp("topLevelProp"), inputRecord.getProp("topLevelProp"));
    Assert.assertEquals(outputSchema.getField("integer1").getProp("innerProp"), "innerVal");
  }


  private Schema.Field getTestInnerRecordField() {
    Schema fieldSchema = SchemaBuilder.record("innerRecord")
            .fields().requiredInt("innerInt").requiredString("innerString")
            .endRecord();
    Schema.Field field = new Schema.Field("innerRecord", fieldSchema, "",null);
    return field;
  }


  @Test
  public void testDecorateSchemaWithSingleRecord() {
    Schema inputRecord = SchemaBuilder.record("test").fields().requiredInt("numeric1")
            .requiredString("string1").endRecord();
    Schema fieldSchema = SchemaBuilder.record("innerRecord")
            .fields().requiredInt("innerInt").requiredString("innerString")
            .endRecord();
    Schema.Field field = new Schema.Field("innerRecord", fieldSchema, "",null);
    Schema outputRecord = AvroUtils.decorateRecordSchema(inputRecord, Collections.singletonList(field));
    checkFieldsMatch(inputRecord, outputRecord);
    Assert.assertNotNull(outputRecord.getField("innerRecord"));
    Assert.assertEquals(outputRecord.getField("innerRecord").schema(), fieldSchema);
  }


  @Test
  public void testDecorateRecordWithPrimitiveField() {
    Schema inputRecordSchema = SchemaBuilder.record("test").fields()
            .name("integer1")
            .prop("innerProp", "innerVal")
            .type().intBuilder().endInt().noDefault()
            .requiredString("string1")
            .endRecord();

    GenericRecord inputRecord = new GenericData.Record(inputRecordSchema);
    inputRecord.put("integer1", 10);
    inputRecord.put("string1", "hello");

    Schema outputRecordSchema = AvroUtils.decorateRecordSchema(inputRecordSchema, Collections.singletonList(new Schema.Field("newField", SchemaBuilder.builder().intType(), "test field", null)));
    Map<String, Object> newFields = new HashMap<>();
    newFields.put("newField", 5);

    GenericRecord outputRecord = AvroUtils.decorateRecord(inputRecord, newFields, outputRecordSchema);
    Assert.assertEquals(outputRecord.get("newField"), 5);
    Assert.assertEquals(outputRecord.get("integer1"), 10);
    Assert.assertEquals(outputRecord.get("string1"), "hello");

  }


  @Test
  public void testDecorateRecordWithNestedField() throws IOException {
    Schema inputRecordSchema = SchemaBuilder.record("test").fields()
            .name("integer1")
            .prop("innerProp", "innerVal")
            .type().intBuilder().endInt().noDefault()
            .requiredString("string1")
            .endRecord();

    GenericRecord inputRecord = new GenericData.Record(inputRecordSchema);
    inputRecord.put("integer1", 10);
    inputRecord.put("string1", "hello");

    Schema nestedFieldSchema = SchemaBuilder.builder().record("metadata")
            .fields()
            .requiredString("source")
            .requiredLong("timestamp")
            .endRecord();

    Schema.Field nestedField = new Schema.Field("metadata", nestedFieldSchema, "I am a nested field", null);

    Schema outputRecordSchema = AvroUtils.decorateRecordSchema(inputRecordSchema, Collections.singletonList(nestedField));
    Map<String, Object> newFields = new HashMap<>();

    GenericData.Record metadataRecord = new GenericData.Record(nestedFieldSchema);
    metadataRecord.put("source", "oracle");
    metadataRecord.put("timestamp", 1234L);

    newFields.put("metadata", metadataRecord);

    GenericRecord outputRecord = AvroUtils.decorateRecord(inputRecord, newFields, outputRecordSchema);
    Assert.assertEquals(outputRecord.get("integer1"), 10);
    Assert.assertEquals(outputRecord.get("string1"), "hello");
    Assert.assertEquals(outputRecord.get("metadata"), metadataRecord);


    // Test that serializing and deserializing this record works.
    GenericDatumWriter writer = new GenericDatumWriter(outputRecordSchema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(baos, null);
    writer.write(outputRecord, binaryEncoder);
    binaryEncoder.flush();
    baos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(bais, null);
    GenericDatumReader reader = new GenericDatumReader(outputRecordSchema);
    GenericRecord deserialized = (GenericRecord) reader.read(null, binaryDecoder);
    Assert.assertEquals(deserialized.get("integer1"), 10);
    Assert.assertEquals(deserialized.get("string1").toString(), "hello"); //extra toString: avro returns Utf8
    Assert.assertEquals(deserialized.get("metadata"), metadataRecord);
  }

  @Test
  public void overrideNameAndNamespaceTest() throws IOException{

    String inputName = "input_name";
    String inputNamespace = "input_namespace";
    String outputName = "output_name";
    String outputNamespace = "output_namespace";

    Schema inputRecordSchema = SchemaBuilder.record(inputName).namespace(inputNamespace).fields()
        .name("integer1")
        .type().intBuilder().endInt().noDefault()
        .endRecord();

    GenericRecord inputRecord = new GenericData.Record(inputRecordSchema);
    inputRecord.put("integer1", 10);

    GenericRecord outputRecord = AvroUtils.overrideNameAndNamespace(inputRecord, outputName, Optional.of(Collections.EMPTY_MAP));
    Assert.assertEquals(outputRecord.getSchema().getName(), outputName);
    Assert.assertEquals(outputRecord.getSchema().getNamespace(), inputNamespace);
    Assert.assertEquals(outputRecord.get("integer1"), 10);

    Map<String,String> namespaceOverrideMap = new HashMap<>();
    namespaceOverrideMap.put(inputNamespace,outputNamespace);

    outputRecord = AvroUtils.overrideNameAndNamespace(inputRecord, outputName, Optional.of(namespaceOverrideMap));
    Assert.assertEquals(outputRecord.getSchema().getName(), outputName);
    Assert.assertEquals(outputRecord.getSchema().getNamespace(), outputNamespace);
    Assert.assertEquals(outputRecord.get("integer1"), 10);

  }

  @Test
  public void overrideSchemaNameAndNamespaceTest() {
    String inputName = "input_name";
    String inputNamespace = "input_namespace";
    String outputName = "output_name";
    String outputNamespace = "output_namespace";

    Schema inputSchema = SchemaBuilder.record(inputName).namespace(inputNamespace).fields()
        .name("integer1")
        .type().intBuilder().endInt().noDefault()
        .endRecord();

    Map<String,String> namespaceOverrideMap = new HashMap<>();
    namespaceOverrideMap.put(inputNamespace, outputNamespace);

    Schema newSchema = AvroUtils.overrideNameAndNamespace(inputSchema, outputName, Optional.of(namespaceOverrideMap));

    Assert.assertEquals(newSchema.getName(), outputName);
    Assert.assertEquals(newSchema.getNamespace(), outputNamespace);
  }


  @Test
  public void testisSchemaRecursive()
      throws IOException {
    for (String scenario : new String[]{"norecursion", "simple", "union", "multiple", "nested", "array", "map"}) {
      System.out.println("Processing scenario for " + scenario);

      Schema inputSchema = new Schema.Parser()
          .parse(getClass().getClassLoader().getResourceAsStream("recursive_schemas/recursive_" + scenario + ".avsc"));

      if (scenario.equals("norecursion")) {
        Assert.assertFalse(AvroUtils.isSchemaRecursive(inputSchema, Optional.of(log)),
            "Schema for scenario " + scenario + " should not be recursive");
      } else {
        Assert.assertTrue(AvroUtils.isSchemaRecursive(inputSchema, Optional.of(log)),
            "Schema for scenario " + scenario + " should be recursive");
      }
    }
  }


  @Test
  public void testDropRecursiveSchema()
      throws IOException {

    for (String scenario : new String[]{"norecursion", "simple", "union", "multiple", "nested", "array", "map"}) {
      System.out.println("Processing scenario for " + scenario);

      Schema inputSchema = new Schema.Parser().parse(getClass().getClassLoader()
          .getResourceAsStream("recursive_schemas/recursive_" + scenario + ".avsc"));

      Schema solutionSchema = new Schema.Parser().parse(getClass().getClassLoader()
          .getResourceAsStream("recursive_schemas/recursive_" + scenario + "_solution.avsc"));

      // get the answer from the input schema (test author needs to provide this)
      ArrayNode foo = (ArrayNode) inputSchema.getJsonProp("recursive_fields");
      HashSet<String> answers = new HashSet<>();
      for (JsonNode fieldsWithRecursion: foo) {
        answers.add(fieldsWithRecursion.getTextValue());
      }

      Pair<Schema, List<AvroUtils.SchemaEntry>> results = AvroUtils.dropRecursiveFields(inputSchema);
      List<AvroUtils.SchemaEntry> fieldsWithRecursion = results.getSecond();
      Schema transformedSchema = results.getFirst();

      // Prove that fields with recursion are no longer present
      for (String answer: answers) {
        Assert.assertFalse(AvroUtils.getField(transformedSchema, answer).isPresent());
      }

      // Additionally compare schema with solution schema
      Assert.assertEquals(solutionSchema, transformedSchema,"Transformed schema differs from solution schema for scenario " + scenario);

      Set<String> recursiveFieldNames = fieldsWithRecursion.stream().map(se -> se.fieldName).collect(Collectors.toSet());
      Assert.assertEquals(recursiveFieldNames, answers,
          "Found recursive fields differ from answers listed in the schema for scenario " + scenario);

    }
  }
}
