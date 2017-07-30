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
package gobblin.converter.http;

import java.io.File;
import java.io.IOException;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.json.JSONException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"gobblin.converter"})
public class AvroToRestJsonEntryConverterTest {

  public void testConversionWithJsonTemplate() throws DataConversionException, IOException, JSONException {
    JsonParser parser = new JsonParser();
    String expectedResourceKey = "/sobject/user/John";
    String expectedJsonStr = "{ \"name\" : \"John\", \"favoriteNumber\" : 9, \"city\" : \"Mountain view\" }";
    RestEntry<JsonObject> expected = new RestEntry<JsonObject>(expectedResourceKey, parser.parse(expectedJsonStr).getAsJsonObject());

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(AvroToRestJsonEntryConverter.CONVERTER_AVRO_REST_ENTRY_RESOURCE_KEY, "/sobject/user/${name}");
    String template = "name=${name},favoriteNumber=${favorite_number},city=${address.city}";
    workUnitState.setProp(AvroToRestJsonEntryConverter.CONVERTER_AVRO_REST_JSON_ENTRY_TEMPLATE, template);

    testConversion(expected, workUnitState);
  }

  public void testConversionWithJsonNestedTemplate() throws DataConversionException, IOException, JSONException {
    JsonParser parser = new JsonParser();
    String expectedResourceKey = "/sobject/user/John";
    String expectedJsonStr = "{ \"name\" : \"John\", \"favoriteNumber\" : 9, \"address\" : { \"city\" : \"Mountain view\"} }";
    RestEntry<JsonObject> expected = new RestEntry<JsonObject>(expectedResourceKey, parser.parse(expectedJsonStr).getAsJsonObject());

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(AvroToRestJsonEntryConverter.CONVERTER_AVRO_REST_ENTRY_RESOURCE_KEY, "/sobject/user/${name}");
    String template = "name=${name},favoriteNumber=${favorite_number},address.city=${address.city}";
    workUnitState.setProp(AvroToRestJsonEntryConverter.CONVERTER_AVRO_REST_JSON_ENTRY_TEMPLATE, template);

    testConversion(expected, workUnitState);
  }

  public void testEqualConversion() throws DataConversionException, IOException, JSONException {
    JsonParser parser = new JsonParser();
    String expectedResourceKey = "/sobject/user/John";
    String expectedJsonStr = "{ \"name\" : \"John\", \"favorite_number\" : 9, \"favorite_color\" : \"blue\", \"date_of_birth\" : 1462387756716, \"last_modified\" : 0, \"created\" : 1462387756716, \"address\" : {\"city\" : \"Mountain view\", \"street_number\" : 2029 } }";
    RestEntry<JsonObject> expected = new RestEntry<JsonObject>(expectedResourceKey, parser.parse(expectedJsonStr).getAsJsonObject());

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(AvroToRestJsonEntryConverter.CONVERTER_AVRO_REST_ENTRY_RESOURCE_KEY, "/sobject/user/${name}");

    testConversion(expected, workUnitState);
  }

  private void testConversion(RestEntry<JsonObject> expected, WorkUnitState actualWorkUnitState) throws DataConversionException, IOException, JSONException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/nested.avsc"));
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);

    File tmp = File.createTempFile(this.getClass().getSimpleName(), null);
    tmp.deleteOnExit();
    try {
      FileUtils.copyInputStreamToFile(getClass().getResourceAsStream("/converter/nested.avro"), tmp);
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(tmp, datumReader);
      GenericRecord avroRecord = dataFileReader.next();

      AvroToRestJsonEntryConverter converter = new AvroToRestJsonEntryConverter();
      RestEntry<JsonObject> actual = converter.convertRecord(null, avroRecord, actualWorkUnitState).iterator().next();

      Assert.assertEquals(actual.getResourcePath(), expected.getResourcePath());
      JSONAssert.assertEquals(expected.getRestEntryVal().toString(), actual.getRestEntryVal().toString(), false);

      converter.close();
      dataFileReader.close();
    } finally {
      if (tmp != null) {
        tmp.delete();
      }
    }
  }
}