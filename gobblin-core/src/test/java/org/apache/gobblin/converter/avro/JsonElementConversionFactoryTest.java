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
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.EnumConverter;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.MapConverter;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.NullConverter;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.RecordConverter;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.StringConverter;
import org.apache.gobblin.converter.json.JsonSchema;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.ArrayConverter;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.JsonElementConverter;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.JsonElementConverter.buildNamespace;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.NULL;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.UnionConverter;


/**
 * Unit test for {@link JsonElementConversionFactory}
 *
 * @author Tilak Patidar
 */
@Test(groups = {"gobblin.converter"})
public class JsonElementConversionFactoryTest {

  private static WorkUnitState state;
  private static JsonObject testData;
  private static JsonParser jsonParser = new JsonParser();

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
    String testName = "schemaWithArrayOfMaps";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();
    JsonSchema jsonSchema = new JsonSchema(schema);
    jsonSchema.setColumnName("dummy");

    ArrayConverter converter = new ArrayConverter(jsonSchema, state);
    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithArrayOfRecords()
      throws Exception {
    String testName = "schemaWithArrayOfRecords";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();
    JsonSchema jsonSchema = new JsonSchema(schema);
    jsonSchema.setColumnName("dummy1");

    ArrayConverter converter = new ArrayConverter(jsonSchema, state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecord()
      throws DataConversionException, SchemaConversionException, UnsupportedDateTypeException {
    String testName = "schemaWithRecord";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();
    JsonSchema jsonSchema = new JsonSchema(schema);
    jsonSchema.setColumnName("dummy1");

    RecordConverter converter =
        new RecordConverter(jsonSchema, state, buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithArrayOfInts()
      throws Exception {
    String testName = "schemaWithArrayOfInts";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    ArrayConverter converter = new ArrayConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithNullType() {
    NullConverter nullConverter = new NullConverter(JsonSchema.buildBaseSchema(NULL));
    JsonObject expected = new JsonObject();
    expected.addProperty("type", "null");
    expected.addProperty("source.type", "null");

    Assert.assertEquals(avroSchemaToJsonElement(nullConverter), expected);
  }

  @Test
  public void schemaWithArrayOfEnums()
      throws Exception {
    String testName = "schemaWithArrayOfEnums";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    ArrayConverter converter = new ArrayConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithMap()
      throws Exception {
    String testName = "schemaWithMap";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    MapConverter converter = new MapConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithMapOfRecords()
      throws Exception {
    String testName = "schemaWithMapOfRecords";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    MapConverter converter = new MapConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithMapOfArrays()
      throws Exception {
    String testName = "schemaWithMapOfArrays";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    MapConverter converter = new MapConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithMapOfEnum()
      throws Exception {
    String testName = "schemaWithMapOfEnum";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    MapConverter converter = new MapConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecordOfMap()
      throws Exception {
    String testName = "schemaWithRecordOfMap";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    RecordConverter converter = new RecordConverter(new JsonSchema(schema), state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecordOfArray()
      throws Exception {
    String testName = "schemaWithRecordOfArray";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    RecordConverter converter = new RecordConverter(new JsonSchema(schema), state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecordOfEnum()
      throws Exception {
    String testName = "schemaWithRecordOfEnum";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    RecordConverter converter = new RecordConverter(new JsonSchema(schema), state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void schemaWithMapValuesAsJsonArray()
      throws Exception {
    String testName = "schemaWithMapValuesAsJsonArray";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();

    new RecordConverter(new JsonSchema(schema), state, buildNamespace(state.getExtract().getNamespace(), "something"));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void schemaWithMapValuesAsJsonNull()
      throws Exception {
    String testName = "schemaWithMapValuesAsJsonNull";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();

    new RecordConverter(new JsonSchema(schema), state, buildNamespace(state.getExtract().getNamespace(), "something"));
  }

  @Test
  public void schemaWithRecordOfRecord()
      throws Exception {
    String testName = "schemaWithRecordOfRecord";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    RecordConverter converter = new RecordConverter(new JsonSchema(schema), state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecordOfRecordCheckNamespace()
      throws Exception {
    String testName = "schemaWithRecordOfRecordCheckNamespace";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    RecordConverter converter =
        new RecordConverter(new JsonSchema(schema), state, buildNamespace(state.getExtract().getNamespace(), "person"));
    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
    Assert.assertEquals(converter.schema().getField("someperson").schema().getNamespace(), "namespace.person.myrecord");
    Assert.assertEquals(converter.schema().getNamespace(), "namespace.person");
  }

  @Test
  public void schemaWithRecordOfEnumCheckNamespace()
      throws Exception {
    String testName = "schemaWithRecordOfEnumCheckNamespace";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonObject expected = getExpectedSchema(testName).getAsJsonObject();

    RecordConverter converter = new RecordConverter(new JsonSchema(schema), state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
    Assert.assertEquals(converter.schema().getField("someperson").schema().getNamespace(),
        "namespace.something.myrecord");
    Assert.assertEquals(converter.schema().getNamespace(), "namespace.something");
  }

  @Test
  public void schemaWithUnion()
      throws Exception {
    String testName = "schemaWithUnion";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonArray expected = getExpectedSchema(testName).getAsJsonArray();

    UnionConverter converter = new UnionConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithComplexUnion()
      throws Exception {
    String testName = "schemaWithComplexUnion";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonArray expected = getExpectedSchema(testName).getAsJsonArray();

    UnionConverter converter = new UnionConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithIsNullable()
      throws Exception {
    String testName = "schemaWithIsNullable";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonArray expected = getExpectedSchema(testName).getAsJsonArray();

    StringConverter converter = new StringConverter(new JsonSchema(schema));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithRecordIsNullable()
      throws Exception {
    String testName = "schemaWithRecordIsNullable";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonArray expected = getExpectedSchema(testName).getAsJsonArray();

    RecordConverter converter = new RecordConverter(new JsonSchema(schema), state,
        buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithMapIsNullable()
      throws Exception {
    String testName = "schemaWithMapIsNullable";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonArray expected = getExpectedSchema(testName).getAsJsonArray();

    MapConverter converter = new MapConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithEnumIsNullable()
      throws Exception {
    String testName = "schemaWithEnumIsNullable";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonArray expected = getExpectedSchema(testName).getAsJsonArray();

    EnumConverter converter = new EnumConverter(new JsonSchema(schema), "something");

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  @Test
  public void schemaWithArrayIsNullable()
      throws Exception {
    String testName = "schemaWithArrayIsNullable";
    JsonObject schema = getSchemaData(testName).getAsJsonObject();
    JsonArray expected = getExpectedSchema(testName).getAsJsonArray();

    ArrayConverter converter = new ArrayConverter(new JsonSchema(schema), state);

    Assert.assertEquals(avroSchemaToJsonElement(converter), expected);
  }

  private JsonElement avroSchemaToJsonElement(JsonElementConverter converter) {
    return jsonParser.parse(converter.schema().toString());
  }

  private JsonElement getExpectedSchema(String methodName) {
    return testData.get(methodName).getAsJsonArray().get(1);
  }

  private JsonElement getSchemaData(String methodName) {
    return testData.get(methodName).getAsJsonArray().get(0);
  }
}