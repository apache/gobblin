/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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
            .parse("{\"type\":\"record\", \"name\":\"test\", "
                + "\"fields\":["
                + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"color\", \"type\": \"string\"}, "
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
}
