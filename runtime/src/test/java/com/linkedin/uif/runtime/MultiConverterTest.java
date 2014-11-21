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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.test.TestConverter;

/**
 * Unit tests for {@link MultiConverter}.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.runtime"})
public class MultiConverterTest {

    private static final String TEST_SCHEMA =
            "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
            "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";

    private static final String TEST_RECORD =
            "{\"fields\": {" +
                    "\"name\": \"Alyssa\", " +
                    "\"favorite_number\": 256, " +
                    "\"favorite_color\": \"yellow\"" +
               "}" +
            "}";

    private MultiConverter<String, String> multiConverter;

    @BeforeClass
    public void setUp() {
        List<Converter<String, ?, String, ?>> converters =
                Lists.newArrayList(
                        new SchemaSimplificationConverter(),
                        new IdentityConverter(),
                        new TestConverter());
        this.multiConverter = new MultiConverter<String, String>(converters);
    }

    @Test
    public void testConversion() throws Exception {
        WorkUnitState workUnitState = new WorkUnitState();

        Schema schema = (Schema) this.multiConverter.convertSchema(
                TEST_SCHEMA, workUnitState);

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

        GenericRecord record = (GenericRecord) this.multiConverter.convertRecord(
                TEST_SCHEMA, TEST_RECORD, workUnitState);

        Assert.assertEquals(record.get("name"), "Alyssa");
        Assert.assertEquals(record.get("favorite_number"), 256d);
        Assert.assertEquals(record.get("favorite_color"), "yellow");
    }

    /**
     * A {@link Converter} that simplifies the input data records.
     */
    private static class SchemaSimplificationConverter
             extends Converter<String, String, String, String> {

        private static final Gson GSON = new Gson();

        @Override
        public String convertSchema(String inputSchema, WorkUnitState workUnit)
                throws SchemaConversionException {

            return inputSchema;
        }

        @Override
        public String convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
                throws DataConversionException {

            JsonElement element = GSON.fromJson(inputRecord, JsonObject.class).get("fields");
            return element.getAsJsonObject().toString();
        }
    }

    /**
     * A {@link Converter} that returns the input schema and data records as they are.
     */
    private static class IdentityConverter extends Converter<String, String, String, String> {

        @Override
        public String convertSchema(String inputSchema, WorkUnitState workUnit)
                throws SchemaConversionException {

            return inputSchema;
        }

        @Override
        public String convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
                throws DataConversionException {

            return inputRecord;
        }
    }
}
