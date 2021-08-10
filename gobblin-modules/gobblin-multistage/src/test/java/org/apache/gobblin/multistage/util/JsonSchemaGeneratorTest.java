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

package org.apache.gobblin.multistage.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import java.io.InputStreamReader;
import java.util.HashMap;
import junit.framework.AssertionFailedError;
import org.apache.gobblin.multistage.util.JsonElementTypes;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.JsonSchemaGenerator;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.powermock.api.mockito.PowerMockito.*;


/**
 * Unit test for {@link JsonSchemaGenerator}
 * @author chrli
 *
 */
@Test(groups = {"org.apache.gobblin.util"})
public class JsonSchemaGeneratorTest {
  private Gson gson;

  @BeforeClass
  public void setup() throws Exception {
    gson = new Gson();
  }

  @Test
  public void testGeneratorWithJsonObject() {

    JsonObject jsonSchema = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/util/sample-schema.json")), JsonObject.class);
    JsonObject jsonData = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/util/sample.json")), JsonObject.class);

    Assert.assertTrue(jsonSchema.equals(new JsonSchemaGenerator(jsonData).getSchema().getSchema()));
  }

  @Test
  public void testGeneratorWithJsonArray() {
    JsonArray jsonData = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/util/calls.json")), JsonArray.class);
    JsonObject jsonSchema = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/util/calls-schema.json")), JsonObject.class);

    Assert.assertEquals(new JsonSchemaGenerator(jsonData).getSchema().getSchema(), jsonSchema);

  }

  @Test
  public void testGeneratorWithNestedJson() {
    JsonArray jsonData = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/util/users.json")), JsonArray.class);
    JsonObject jsonSchema = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/util/users-schema.json")), JsonObject.class);

    Assert.assertEquals(new JsonSchemaGenerator(jsonData).getSchema().getSchema(), jsonSchema);

  }

  /**
   * a field, or a nested field, should be considered Nullable when any of following is true
   * 1. the data is NULL
   * 2. the data is an empty primitive
   * 3. the data is an empty JsonObject
   * 4. the data is an empty JsonArray
   * 5. the data is JsonNull
   */
  @Test
  public void testIsEmpty() {
    JsonArray empty = gson.fromJson("[\"\"]", JsonArray.class);
    Assert.assertTrue(new JsonSchemaGenerator(new JsonArray()).isEmpty(null));
    Assert.assertTrue(new JsonSchemaGenerator(new JsonArray()).isEmpty(new JsonObject()));
    Assert.assertTrue(new JsonSchemaGenerator(new JsonArray()).isEmpty(new JsonArray()));
    Assert.assertTrue(new JsonSchemaGenerator(new JsonArray()).isEmpty(new JsonNull()));
    Assert.assertTrue(new JsonSchemaGenerator(new JsonArray()).isEmpty(empty.get(0)));
  }

  /**
   * getJsonElementType converts Json schema elements to JsonElementTypes
   * input  1: JsonArray
   * output 1: JsonElementTypes.ARRAY
   *
   * input  2: JsonObject
   * output 2: JsonElementTypes.OBJECT
   *
   * input  3: JsonNull
   * output 3: JsonElementTypes.NULL
   *
   * input  4: JsonPrimitive
   * output 4: JsonElementTypes.PRIMITIVE
   */
  @Test
  public void testGetJsonElementType() {
    JsonArray jsonArray = gson.fromJson("[\"dummy\"]", JsonArray.class);
    JsonObject jsonObject = gson.fromJson("{\"type\": \"int\"}", JsonObject.class);
    Assert.assertEquals(new JsonSchemaGenerator(new JsonArray()).getJsonElementType(jsonArray), JsonElementTypes.ARRAY);
    Assert.assertEquals(new JsonSchemaGenerator(new JsonArray()).getJsonElementType(jsonObject), JsonElementTypes.OBJECT);
    Assert.assertEquals(new JsonSchemaGenerator(new JsonArray()).getJsonElementType(new JsonNull()), JsonElementTypes.NULL);
    Assert.assertEquals(new JsonSchemaGenerator(new JsonArray()).getJsonElementType(jsonArray.get(0)), JsonElementTypes.PRIMITIVE);
  }

  /**
   *  Check if a value is a sub table, the input of this is the value part of the first element in an column array
   *
   *  Input  1: a name object
   *  Output 1: true
   *
   *  Input  2: a blank object
   *  Output 2: false
   *
   *  Input  3: an array of aliases
   *  Output 2: false
   */
  @Test
  public void testIsSubtable() {
    JsonArray jsonArray = gson.fromJson("[{\"name\": {\"firstName\": \"fn\", \"lastName\": \"ln\"}}]", JsonArray.class);
    Assert.assertTrue(new JsonSchemaGenerator(new JsonArray()).isSubTable(jsonArray.get(0).getAsJsonObject().get("name")));

    jsonArray = gson.fromJson("[{\"name\": {}}]", JsonArray.class);
    Assert.assertFalse(new JsonSchemaGenerator(new JsonArray()).isSubTable(jsonArray.get(0).getAsJsonObject().get("name")));

    jsonArray = gson.fromJson("[{\"aliases\": [\"alias1\", \"alias2\"]}]", JsonArray.class);
    Assert.assertFalse(new JsonSchemaGenerator(new JsonArray()).isSubTable(jsonArray.get(0).getAsJsonObject().get("aliases")));
  }

  /***
   * Test pivoting array data so that it becomes a columnar format
   *
   * Input  1: empty array
   * Output 1: empty array
   *
   * Input  2: primitive array
   * Output 2: primitive array
   *
   * Input  3: array of objects
   * Output 3: columnar format array
   */
  @Test
  public void testPivotJsonArray() {
    JsonArray jsonArray1 = gson.fromJson("[]", JsonArray.class);
    Assert.assertEquals(new JsonSchemaGenerator(new JsonArray()).pivotJsonArray(jsonArray1), jsonArray1);

    JsonArray jsonArray2 = gson.fromJson("[\"a\", \"b\", \"c\"]", JsonArray.class);
    Assert.assertEquals(new JsonSchemaGenerator(new JsonArray()).pivotJsonArray(jsonArray2), jsonArray2);

    JsonArray jsonArray3 = gson.fromJson("[{\"column2\": \"x\"}, {\"column1\": \"b\", \"column2\": \"y\"}, {\"column1\": \"c\", \"column2\": \"z\"}]", JsonArray.class);
    JsonArray expected3 = gson.fromJson("[[{\"column2\": \"x\"}, {\"column2\": \"y\"}, {\"column2\": \"z\"}], [{\"column1\": \"b\"}, {\"column1\": \"c\"}]]", JsonArray.class);
    Assert.assertEquals(new JsonSchemaGenerator(new JsonArray()).pivotJsonArray(jsonArray3), expected3);

    JsonArray jsonArray4 = gson.fromJson("[[\"a\", \"b\", \"c\"]]", JsonArray.class);
    Assert.assertEquals(new JsonSchemaGenerator(new JsonArray()).pivotJsonArray(jsonArray4), new JsonArray());

    JsonArray jsonArray5 = gson.fromJson("[{}, {\"column1\": \"b\", \"column2\": \"y\"}, {\"column1\": \"c\", \"column2\": \"z\"}]", JsonArray.class);
    JsonArray expected5 = gson.fromJson("[[null, {\"column1\": \"b\"}, {\"column1\": \"c\"}], [null, {\"column2\": \"y\"}, {\"column2\": \"z\"}]]", JsonArray.class);
    Assert.assertEquals(new JsonSchemaGenerator(new JsonArray()).pivotJsonArray(jsonArray5), expected5);
  }

  /***
   * Test schema inference
   *
   * Input  1: empty array
   * Output 1: dummy schema
   *
   * Input  2: primitive array
   * Output 2: dummy schema
   *
   * Input  3: array of objects
   * Output 3: normal json schema
   */
  @Test
  public void testGetSchema() {
    JsonArray jsonArray = gson.fromJson("[]", JsonArray.class);
    JsonSchema schema = new JsonSchemaGenerator(jsonArray, true).getSchema();
    Assert.assertEquals(schema.toString(), "{\"type\":\"array\",\"items\":{\"type\":\"null\"}}");
    Assert.assertEquals(schema.getAltSchema(new HashMap<>(), true).toString(),
        "[{\"columnName\":\"dummy\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]");

    jsonArray = gson.fromJson("[\"a\", \"b\", \"c\"]", JsonArray.class);
    schema = new JsonSchemaGenerator(jsonArray, true).getSchema();
    Assert.assertEquals(schema.toString(), "{\"type\":\"array\",\"items\":{\"type\":\"string\"}}");
    Assert.assertEquals(schema.getAltSchema(new HashMap<>(), true).toString(),
        "[{\"columnName\":\"dummy\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]");

    jsonArray = gson.fromJson("[{\"column2\": \"x\"}, {\"column1\": \"b\", \"column2\": \"y\"}, {\"column1\": \"c\", \"column2\": \"z\"}]", JsonArray.class);
    schema = new JsonSchemaGenerator(jsonArray, false).getSchema();
    Assert.assertEquals(schema.toString(), "{\"type\":\"array\",\"items\":{\"column2\":{\"type\":\"string\"},\"column1\":{\"type\":\"string\"}}}");

    jsonArray = gson.fromJson("[1, 2]", JsonArray.class);
    schema = new JsonSchemaGenerator(jsonArray, true).getSchema();
    Assert.assertEquals(schema.toString(), "{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}");

    jsonArray = gson.fromJson("[1.0, 2.0]", JsonArray.class);
    schema = new JsonSchemaGenerator(jsonArray, true).getSchema();
    Assert.assertEquals(schema.toString(), "{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");

    jsonArray = gson.fromJson("[true, false]", JsonArray.class);
    schema = new JsonSchemaGenerator(jsonArray, true).getSchema();
    Assert.assertEquals(schema.toString(), "{\"type\":\"array\",\"items\":{\"type\":\"boolean\"}}");
  }

  @Test
  public void testInferSchemaFromMultiple() throws Exception {
    JsonElement element = mock(JsonElement.class);
    JsonSchemaGenerator generator = new JsonSchemaGenerator(element);
    JsonArray data = new JsonArray();
    JsonSchema actual = Whitebox.invokeMethod(generator, "inferSchemaFromMultiple", data);
    Assert.assertEquals(actual.toString(), "{\"type\":null}");

    data = gson.fromJson("[null,\"some\"]", JsonArray.class);
    actual = Whitebox.invokeMethod(generator, "inferSchemaFromMultiple", data);
    Assert.assertEquals(actual.toString(), "{\"type\":[\"string\",\"null\"]}");
  }

  @Test
  public void testInferArraySchemaFromMultiple() throws Exception {
    JsonSchemaGenerator generator = new JsonSchemaGenerator(mock(JsonElement.class));
    String expected = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"type\":\"null\"}}}";

    JsonArray data = gson.fromJson("[null,[{\"key\":\"value\"}]]", JsonArray.class);
    JsonSchema actual = Whitebox.invokeMethod(generator, "inferArraySchemaFromMultiple", data);
    Assert.assertEquals(actual.toString(), expected);

    data = gson.fromJson("[[],[{\"key\":\"value\"}]]", JsonArray.class);
    actual = Whitebox.invokeMethod(generator, "inferArraySchemaFromMultiple", data);
    Assert.assertEquals(actual.toString(), expected);
  }

  @Test
  public void testGetValueArray() throws Exception {
    JsonSchemaGenerator generator = new JsonSchemaGenerator(mock(JsonElement.class));
    JsonArray kvArray = gson.fromJson("[null,{\"category\":\"authentication\"}]", JsonArray.class);
    Assert.assertEquals(generator.getValueArray(kvArray).toString(),
        "[null,\"authentication\"]");
  }
}