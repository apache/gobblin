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

package gobblin.util;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroUtilsTest {
  private static final String AVRO_DIR = "gobblin-utility/src/test/resources/avroDirParent/";

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

  private List<GenericRecord> getRecordFromFile(String path)
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
   * This is a test to validate support of maps in {@link gobblin.util.AvroUtils#getFieldValue(GenericRecord, String)}
   * and {@link gobblin.util.AvroUtils#getFieldSchema(Schema, String)}
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
}
