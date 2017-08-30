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

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.MapConverter;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.NullConverter;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.RecordConverter;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.testng.Assert;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.ArrayConverter;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.JsonElementConverter;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.JsonElementConverter.buildNamespace;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.ARRAY;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.MAP;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.RECORD;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.UNION;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.UnionConverter;


/**
 * Unit test for {@link JsonElementConversionFactory}
 *
 * @author Tilak Patidar
 */
public class JsonElementConversionFactoryTest {

  private static WorkUnitState state;
  private static JsonObject testData;
  private static JsonParser jsonParser = new JsonParser();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() {
    WorkUnit workUnit = new WorkUnit(new SourceState(),
        new Extract(new SourceState(), Extract.TableType.SNAPSHOT_ONLY, "namespace", "dummy_table"));
    state = new WorkUnitState(workUnit);
    Type listType = new TypeToken<JsonObject>() {
    }.getType();
    Gson gson = new Gson();
    testData = gson.fromJson(new InputStreamReader(
            JsonElementConversionFactoryTest.class.getResourceAsStream("/converter/JsonElementConversionFactoryTest.json")),
        listType);
  }

  @Test
  public void schemaWithArrayOfMaps()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    ArrayConverter converter = new ArrayConverter("dummy", true, ARRAY.toString(), schema, state);
    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithArrayOfRecords()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    ArrayConverter converter = new ArrayConverter("dummy1", true, ARRAY.toString(), schema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecord()
      throws DataConversionException, SchemaConversionException, UnsupportedDateTypeException {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    RecordConverter converter = new RecordConverter("dummy1", true, RECORD.toString(), schema, state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithArrayOfInts()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    ArrayConverter converter = new ArrayConverter("dummy1", true, ARRAY.toString(), schema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithNullType() {
    NullConverter nullConverter = new NullConverter("dummy1", ARRAY.toString());
    JsonObject expected = new JsonObject();
    expected.addProperty("type", "null");
    expected.addProperty("source.type", "array");

    Assert.assertEquals(avroSchemaToJsonElement(nullConverter), expected);
  }

  @Test
  public void schemaWithArrayOfEnums()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    ArrayConverter converter = new ArrayConverter("dummy1", true, ARRAY.toString(), schema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithMap()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    MapConverter converter = new MapConverter("dummy1", true, MAP.toString(), schema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithMapOfRecords()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    MapConverter converter = new MapConverter("dummy1", true, MAP.toString(), schema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithMapOfArrays()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    MapConverter converter = new MapConverter("dummy1", true, MAP.toString(), schema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithMapOfEnum()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    MapConverter converter = new MapConverter("dummy1", true, MAP.toString(), schema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecordOfMap()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    RecordConverter converter = new RecordConverter("dummy1", true, RECORD.toString(), schema, state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecordOfArray()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    RecordConverter converter = new RecordConverter("dummy1", true, RECORD.toString(), schema, state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecordOfEnum()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    RecordConverter converter = new RecordConverter("dummy1", true, RECORD.toString(), schema, state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void schemaWithMapValuesAsJsonArray()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();

    new RecordConverter("dummy1", true, RECORD.toString(), schema, state,
        buildNamespace(state.getExtract().getNamespace(), "something"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void schemaWithMapValuesAsJsonNull()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();

    new RecordConverter("dummy1", true, RECORD.toString(), schema, state,
        buildNamespace(state.getExtract().getNamespace(), "something"));
  }

  @Test
  public void schemaWithRecordOfRecord()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    RecordConverter converter = new RecordConverter("dummy1", true, RECORD.toString(), schema, state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecordOfRecordCheckNamespace()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    RecordConverter converter = new RecordConverter("dummy1", true, RECORD.toString(), schema, state,
        buildNamespace(state.getExtract().getNamespace(), "person"));
    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
    Assert.assertEquals(converter.schema().getField("someperson").schema().getNamespace(), "namespace.person.myrecord");
    Assert.assertEquals(converter.schema().getNamespace(), "namespace.person");
  }

  @Test
  public void schemaWithRecordOfEnumCheckNamespace()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonObject expected = getExpectedSchema().getAsJsonObject();

    RecordConverter converter = new RecordConverter("dummy1", true, RECORD.toString(), schema, state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
    Assert.assertEquals(converter.schema().getField("someperson").schema().getNamespace(),
        "namespace.something.myrecord");
    Assert.assertEquals(converter.schema().getNamespace(), "namespace.something");
  }

  @Test
  public void schemaWithUnion()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonArray expected = getExpectedSchema().getAsJsonArray();

    UnionConverter converter = new UnionConverter("dummy1", UNION.toString(), schema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithComplexUnion()
      throws Exception {
    JsonObject schema = getSchemaData().getAsJsonObject();
    JsonArray expected = getExpectedSchema().getAsJsonArray();

    UnionConverter converter = new UnionConverter("dummy1", UNION.toString(), schema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  private JsonElement avroSchemaToJsonElement(JsonElementConverter converter) {
    return jsonParser.parse(converter.schema().toString());
  }

  private JsonElement getExpectedSchema() {
    return testData.get(name.getMethodName()).getAsJsonArray().get(1);
  }

  private JsonElement getSchemaData() {
    return testData.get(name.getMethodName()).getAsJsonArray().get(0);
  }
}