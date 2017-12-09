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

package org.apache.gobblin.converter.avro;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;


/**
 * Unit test for {@link JsonIntermediateToAvroConverter}
 * @author kgoodhop
 *
 */
@Test(groups = {"gobblin.converter"})
public class JsonIntermediateToAvroConverterTest {
  private JsonArray jsonSchema;
  private JsonObject jsonRecord;
  private WorkUnitState state;

  /**
   * To test schema and record using the path to their resource file.
   * @param resourceFilePath
   * @throws SchemaConversionException
   * @throws DataConversionException
   */
  private void complexSchemaTest(String resourceFilePath)
      throws SchemaConversionException, DataConversionException {
    JsonObject testData = initResources(resourceFilePath);

    JsonIntermediateToAvroConverter converter = new JsonIntermediateToAvroConverter();

    Schema avroSchema = converter.convertSchema(jsonSchema, state);
    GenericRecord genericRecord = converter.convertRecord(avroSchema, jsonRecord, state).iterator().next();
    JsonParser parser = new JsonParser();
    Assert.assertEquals(parser.parse(avroSchema.toString()).getAsJsonObject(),
        testData.get("expectedSchema").getAsJsonObject());
    Assert.assertEquals(parser.parse(genericRecord.toString()), testData.get("expectedRecord").getAsJsonObject());
  }

  private JsonObject initResources(String resourceFilePath) {
    Type listType = new TypeToken<JsonObject>() {
    }.getType();
    Gson gson = new Gson();
    JsonObject testData =
        gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream(resourceFilePath)), listType);

    jsonRecord = testData.get("record").getAsJsonObject();
    jsonSchema = testData.get("schema").getAsJsonArray();

    WorkUnit workUnit = new WorkUnit(new SourceState(),
        new Extract(new SourceState(), Extract.TableType.SNAPSHOT_ONLY, "namespace", "dummy_table"));
    state = new WorkUnitState(workUnit);
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_TIME_FORMAT, "HH:mm:ss");
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_DATE_TIMEZONE, "PST");
    return testData;
  }

  @Test
  public void testConverter()
      throws Exception {
    initResources("/converter/schema.json");
    JsonIntermediateToAvroConverter converter = new JsonIntermediateToAvroConverter();

    Schema avroSchema = converter.convertSchema(jsonSchema, state);
    GenericRecord record = converter.convertRecord(avroSchema, jsonRecord, state).iterator().next();

    //testing output values are expected types and values
    Assert.assertEquals(jsonRecord.get("Id").getAsString(), record.get("Id").toString());
    Assert.assertEquals(jsonRecord.get("IsDeleted").getAsBoolean(), record.get("IsDeleted"));

    if (!(record.get("Salutation") instanceof GenericArray)) {
      Assert.fail("expected array, found " + record.get("Salutation").getClass().getName());
    }

    if (!(record.get("MapAccount") instanceof Map)) {
      Assert.fail("expected map, found " + record.get("MapAccount").getClass().getName());
    }

    Assert.assertEquals(jsonRecord.get("Industry").getAsString(), record.get("Industry").toString());

    DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));

    Assert.assertEquals(jsonRecord.get("LastModifiedDate").getAsString(),
        new DateTime(record.get("LastModifiedDate")).toString(format));
    Assert.assertEquals(jsonRecord.get("date_type").getAsString(),
        new DateTime(record.get("date_type")).toString(format));

    format = DateTimeFormat.forPattern("HH:mm:ss").withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
    Assert.assertEquals(jsonRecord.get("time_type").getAsString(),
        new DateTime(record.get("time_type")).toString(format));
    Assert.assertEquals(jsonRecord.get("bytes_type").getAsString().getBytes(),
        ((ByteBuffer) record.get("bytes_type")).array());
    Assert.assertEquals(jsonRecord.get("int_type").getAsInt(), record.get("int_type"));
    Assert.assertEquals(jsonRecord.get("long_type").getAsLong(), record.get("long_type"));
    Assert.assertEquals(jsonRecord.get("float_type").getAsFloat(), record.get("float_type"));
    Assert.assertEquals(jsonRecord.get("double_type").getAsDouble(), record.get("double_type"));

    //Testing timezone
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_DATE_TIMEZONE, "EST");
    avroSchema = converter.convertSchema(jsonSchema, state);
    GenericRecord record2 = converter.convertRecord(avroSchema, jsonRecord, state).iterator().next();

    Assert.assertNotEquals(record.get("LastModifiedDate"), record2.get("LastModifiedDate"));
  }

  @Test
  public void testComplexSchema1()
      throws Exception {
    complexSchemaTest("/converter/complex1.json");
  }

  @Test
  public void testComplexSchema2()
      throws Exception {
    complexSchemaTest("/converter/complex2.json");
  }

  @Test
  public void testComplexSchema3()
      throws Exception {
    complexSchemaTest("/converter/complex3.json");
  }
}
