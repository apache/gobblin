/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.runtime;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.EmptyIterable;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.converter.SingleRecordIterable;
import com.linkedin.uif.test.TestConverter;


/**
 * Unit tests for {@link MultiConverter}.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.runtime"})
public class MultiConverterTest {

  private static final String TEST_SCHEMA = "{\"namespace\": \"example.avro\",\n" +
      " \"type\": \"record\",\n" +
      " \"name\": \"User\",\n" +
      " \"fields\": [\n" +
      "     {\"name\": \"name\", \"type\": \"string\"},\n" +
      "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
      "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
      " ]\n" +
      "}";

  private static final String TEST_RECORD = "{\"fields\": {" +
      "\"name\": \"Alyssa\", " +
      "\"favorite_number\": 256, " +
      "\"favorite_color\": \"yellow\"" +
      "}" +
      "}";

  @Test
  public void testConversion()
      throws Exception {
    MultiConverter multiConverter = new MultiConverter(
        Lists.newArrayList(new SchemaSimplificationConverter(), new IdentityConverter(), new TestConverter()));
    WorkUnitState workUnitState = new WorkUnitState();

    Schema schema = (Schema) multiConverter.convertSchema(TEST_SCHEMA, workUnitState);
    Iterable<Object> convertedRecordIterable = multiConverter.convertRecord(schema, TEST_RECORD, workUnitState);
    Assert.assertEquals(Iterables.size(convertedRecordIterable), 1);
    checkConvertedAvroData(schema, (GenericRecord) convertedRecordIterable.iterator().next());
  }

  @Test
  public void testConversionWithMultiplicity()
      throws Exception {
    MultiConverter multiConverter = new MultiConverter(Lists
        .newArrayList(new SchemaSimplificationConverter(), new MultiIdentityConverter(2), new MultiIdentityConverter(2),
            new TestConverter()));
    WorkUnitState workUnitState = new WorkUnitState();

    Schema schema = (Schema) multiConverter.convertSchema(TEST_SCHEMA, workUnitState);
    Iterable<Object> convertedRecordIterable = multiConverter.convertRecord(schema, TEST_RECORD, workUnitState);
    Assert.assertEquals(Iterables.size(convertedRecordIterable), 4);
    for (Object record : convertedRecordIterable) {
      checkConvertedAvroData(schema, (GenericRecord) record);
    }
  }

  @Test
  public void testConversionWithEmptyConverter()
      throws Exception {
    WorkUnitState workUnitState = new WorkUnitState();

    MultiConverter multiConverter = new MultiConverter(
        Lists.newArrayList(new EmptyConverter(), new SchemaSimplificationConverter(), new TestConverter()));
    Schema schema = (Schema) multiConverter.convertSchema(TEST_SCHEMA, workUnitState);
    Assert.assertFalse(multiConverter.convertRecord(schema, TEST_RECORD, workUnitState).iterator().hasNext());

    multiConverter = new MultiConverter(
        Lists.newArrayList(new SchemaSimplificationConverter(), new EmptyConverter(), new TestConverter()));
    schema = (Schema) multiConverter.convertSchema(TEST_SCHEMA, workUnitState);
    Assert.assertFalse(multiConverter.convertRecord(schema, TEST_RECORD, workUnitState).iterator().hasNext());

    multiConverter = new MultiConverter(
        Lists.newArrayList(new SchemaSimplificationConverter(), new TestConverter(), new EmptyConverter()));
    schema = (Schema) multiConverter.convertSchema(TEST_SCHEMA, workUnitState);
    Assert.assertFalse(multiConverter.convertRecord(schema, TEST_RECORD, workUnitState).iterator().hasNext());
  }

  @Test
  public void testConversionWithoutConverters()
      throws Exception {
    MultiConverter multiConverter = new MultiConverter(Lists.<Converter<? extends Object, ? extends Object, ? extends Object, ? extends Object>>newArrayList());
    WorkUnitState workUnitState = new WorkUnitState();
    Assert.assertEquals(TEST_SCHEMA, multiConverter.convertSchema(TEST_SCHEMA, workUnitState));
    Assert.assertEquals(TEST_RECORD,
        multiConverter.convertRecord(TEST_SCHEMA, TEST_RECORD, workUnitState).iterator().next());
  }

  private void checkConvertedAvroData(Schema schema, GenericRecord record) {
    Assert.assertEquals(schema.getNamespace(), "example.avro");
    Assert.assertEquals(schema.getType(), Schema.Type.RECORD);
    Assert.assertEquals(schema.getName(), "User");
    Assert.assertEquals(schema.getFields().size(), 3);

    Schema.Field nameField = schema.getField("name");
    Assert.assertEquals(nameField.name(), "name");
    Assert.assertEquals(nameField.schema().getType(), Schema.Type.STRING);

    Schema.Field favNumberField = schema.getField("favorite_number");
    Assert.assertEquals(favNumberField.name(), "favorite_number");
    Assert.assertEquals(favNumberField.schema().getType(), Schema.Type.INT);

    Schema.Field favColorField = schema.getField("favorite_color");
    Assert.assertEquals(favColorField.name(), "favorite_color");
    Assert.assertEquals(favColorField.schema().getType(), Schema.Type.STRING);

    Assert.assertEquals(record.get("name"), "Alyssa");
    Assert.assertEquals(record.get("favorite_number"), 256d);
    Assert.assertEquals(record.get("favorite_color"), "yellow");
  }

  /**
   * A {@link Converter} that simplifies the input data records.
   */
  private static class SchemaSimplificationConverter extends Converter<String, String, String, String> {

    private static final Gson GSON = new Gson();

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit)
        throws SchemaConversionException {
      return inputSchema;
    }

    @Override
    public Iterable<String> convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      JsonElement element = GSON.fromJson(inputRecord, JsonObject.class).get("fields");
      return new SingleRecordIterable<String>(element.getAsJsonObject().toString());
    }
  }

  /**
   * A {@link Converter} that returns the input schema and data records as they are.
   */
  private static class IdentityConverter extends Converter<Object, Object, Object, Object> {

    @Override
    public Object convertSchema(Object inputSchema, WorkUnitState workUnit)
        throws SchemaConversionException {
      return inputSchema;
    }

    @Override
    public Iterable<Object> convertRecord(Object outputSchema, Object inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      return new SingleRecordIterable<Object>(inputRecord);
    }
  }

  /**
   * A {@link Converter} that returns the input schema and data records as they are but with a given multiplicity.
   */
  private static class MultiIdentityConverter extends Converter<Object, Object, Object, Object> {

    private final int multiplicity;

    public MultiIdentityConverter(int multiplicity) {
      this.multiplicity = multiplicity;
    }

    @Override
    public Object convertSchema(Object inputSchema, WorkUnitState workUnit)
        throws SchemaConversionException {
      return inputSchema;
    }

    @Override
    public Iterable<Object> convertRecord(Object outputSchema, Object inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      List<Object> records = Lists.newArrayList();
      for (int i = 0; i < this.multiplicity; i++) {
        records.add(inputRecord);
      }
      return records;
    }
  }

  /**
   * A {@link Converter} that returns no converted data record.
   */
  private static class EmptyConverter extends Converter<Object, Object, Object, Object> {

    @Override
    public Object convertSchema(Object inputSchema, WorkUnitState workUnit)
        throws SchemaConversionException {
      return inputSchema;
    }

    @Override
    public Iterable<Object> convertRecord(Object outputSchema, Object inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      return new EmptyIterable<Object>();
    }
  }
}
