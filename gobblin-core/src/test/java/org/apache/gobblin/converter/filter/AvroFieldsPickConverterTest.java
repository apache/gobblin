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

package org.apache.gobblin.converter.filter;

import java.io.File;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = { "gobblin.converter.filter" })
public class AvroFieldsPickConverterTest {

  @Test
  public void testFieldsPick() throws Exception {

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickInput.avsc"));

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS, "name,favorite_number,favorite_color");

    try (AvroFieldsPickConverter converter = new AvroFieldsPickConverter()) {
      Schema converted = converter.convertSchema(inputSchema, workUnitState);
      Schema expected = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickExpected.avsc"));

      JSONAssert.assertEquals(expected.toString(), converted.toString(), false);
    }
  }

  @Test (expectedExceptions=SchemaConversionException.class)
  public void testFieldsPickWrongFieldFailure() throws Exception {

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickInput.avsc"));

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS, "name,favorite_number,favorite_food");

    try (AvroFieldsPickConverter converter = new AvroFieldsPickConverter()) {
      Schema converted = converter.convertSchema(inputSchema, workUnitState);
      Schema expected = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickExpected.avsc"));

      JSONAssert.assertEquals(expected.toString(), converted.toString(), false);
    }
  }

  @Test
  public void testFieldsPickWithNestedRecord() throws Exception {
    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/pickfields_nested_with_union.avsc"));

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS, "name,favorite_number,nested1.nested1_string,nested1.nested2_union.nested2_string");

    try (AvroFieldsPickConverter converter = new AvroFieldsPickConverter()) {
      Schema convertedSchema = converter.convertSchema(inputSchema, workUnitState);
      Schema expectedSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/converted_pickfields_nested_with_union.avsc"));
      JSONAssert.assertEquals(expectedSchema.toString(), convertedSchema.toString(), false);

      try (DataFileReader<GenericRecord> srcDataFileReader = new DataFileReader<GenericRecord>(
              new File(getClass().getResource("/converter/pickfields_nested_with_union.avro").toURI()),
                  new GenericDatumReader<GenericRecord>(inputSchema));
          DataFileReader<GenericRecord> expectedDataFileReader = new DataFileReader<GenericRecord>(
              new File(getClass().getResource("/converter/converted_pickfields_nested_with_union.avro").toURI()),
                  new GenericDatumReader<GenericRecord>(expectedSchema));) {

        while (expectedDataFileReader.hasNext()) {
          GenericRecord expected = expectedDataFileReader.next();
          GenericRecord actual = converter.convertRecord(convertedSchema, srcDataFileReader.next(), workUnitState).iterator().next();
          Assert.assertEquals(actual, expected);
        }
        Assert.assertTrue(!srcDataFileReader.hasNext());
      }
    }
  }
}