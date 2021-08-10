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
import com.google.gson.JsonObject;
import java.io.InputStreamReader;
import org.apache.gobblin.multistage.util.JsonElementTypes;
import org.apache.gobblin.multistage.util.JsonIntermediateSchema;
import org.junit.Assert;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class JsonIntermediateSchemaTest {
  private Gson gson = new Gson();

  @Mock
  private JsonIntermediateSchema jsonIntermediateSchema;

  @BeforeClass
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCreateSchemaFromJson() {
    JsonArray schemaJson = gson.fromJson(
        new InputStreamReader(this.getClass().getResourceAsStream("/json/sample-intermediate-json-schema.json")),
        JsonArray.class);
    JsonIntermediateSchema schema = new JsonIntermediateSchema(schemaJson);
    Assert.assertEquals(schema.toJson().size(), schemaJson.size());
    Assert.assertEquals(schema.getColumns().get("context").getDataType().getType().toString(), "array");
    Assert.assertEquals(schema.getColumns()
        .get("context")
        .getDataType()
        .getItemType()
        .getChildRecord()
        .getColumns()
        .get("objects")
        .getDataType()
        .getType()
        .toString(), "array");
    Assert.assertEquals(schema.getColumns()
        .get("context")
        .getDataType()
        .getItemType()
        .getChildRecord()
        .getColumns()
        .get("objects")
        .getDataType()
        .getItemType()
        .getChildRecord()
        .getColumns()
        .get("fields")
        .getDataType()
        .getItemType()
        .getChildRecord()
        .getColumns()
        .get("count")
        .getDataType()
        .getType()
        .toString(), "long");
  }

  @Test
  public void test_jisColumn_first_constructor() {
    String jsonElementTypeString = "[{\"City\":\"Seattle\"}]";
    JsonIntermediateSchema.JisColumn jisColumn =
        jsonIntermediateSchema.new JisColumn("name", true, jsonElementTypeString);
    Assert.assertEquals("name", jisColumn.getColumnName());
    Assert.assertEquals(true, jisColumn.getIsNullable());
    Assert.assertEquals(JsonElementTypes.UNION, jisColumn.getDataType().type);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void test_jisColumn_second_constructor_failed() {
    JsonObject obj = new JsonObject();
    obj.addProperty("name", "tester");
    jsonIntermediateSchema.new JisColumn(obj);
  }

  @Test
  public void test_jisColumn_second_constructor_succeeded() {
    JsonObject dataTypeObj = new JsonObject();
    dataTypeObj.addProperty("name", "tester");
    dataTypeObj.addProperty("type", "[[\"name\"]]");
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("symbolA");
    jsonArray.add("symbolB");
    dataTypeObj.add("symbols", jsonArray);

    JsonObject obj = new JsonObject();
    obj.addProperty("columnName", "testColumn");
    obj.addProperty("isNullable", false);
    obj.add("dataType", dataTypeObj);
    JsonIntermediateSchema.JisColumn jisColumn = jsonIntermediateSchema.new JisColumn(obj);
    Assert.assertEquals("testColumn", jisColumn.getColumnName());
    Assert.assertEquals(false, jisColumn.isNullable);
    Assert.assertEquals(JsonElementTypes.UNION, jisColumn.getDataType().getType());

    obj = new JsonObject();
    dataTypeObj.addProperty("type", "ENUM");
    obj.add("dataType", dataTypeObj);
    jisColumn = jsonIntermediateSchema.new JisColumn(obj);
    Assert.assertEquals("unknown", jisColumn.getColumnName());
    Assert.assertEquals(true, jisColumn.isNullable);
    Assert.assertEquals(JsonElementTypes.ENUM, jisColumn.getDataType().getType());
    Assert.assertEquals(jsonArray, jisColumn.getDataType().getSymbols());
    Assert.assertEquals("tester", jisColumn.getDataType().getName());
  }

  @Test
  public void test_JsonIntermediateSchema_constructor_succeeded() {
    JsonObject dataTypeObj = new JsonObject();
    dataTypeObj.addProperty("name", "tester");
    dataTypeObj.addProperty("type", "[[\"name\"]]");
    JsonArray jsonArray = new JsonArray();
    JsonObject obj = new JsonObject();
    obj.addProperty("columnName", "testColumn");
    obj.addProperty("isNullable", false);
    obj.add("dataType", dataTypeObj);
    jsonArray.add(obj);

    JsonIntermediateSchema jsonIntermediateSchema = new JsonIntermediateSchema(jsonArray);
    Assert.assertEquals(jsonIntermediateSchema.schemaName, "root");
    JsonIntermediateSchema.JisColumn jisColumn = jsonIntermediateSchema.getColumns().get("testColumn");
    Assert.assertEquals("testColumn", jisColumn.getColumnName());
    Assert.assertEquals(false, jisColumn.getIsNullable());
    Assert.assertEquals(JsonElementTypes.UNION, jisColumn.getDataType().getType());
    Assert.assertEquals("tester", jisColumn.getDataType().name);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void test_JsonIntermediateSchema_constructor_non_object_failed() {
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("tester");
    new JsonIntermediateSchema(jsonArray);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void test_JsonIntermediateSchema_constructor_non_failed() {
    JsonArray jsonArray = new JsonArray();
    jsonArray.add((JsonElement) null);
    new JsonIntermediateSchema(jsonArray);
  }
}
