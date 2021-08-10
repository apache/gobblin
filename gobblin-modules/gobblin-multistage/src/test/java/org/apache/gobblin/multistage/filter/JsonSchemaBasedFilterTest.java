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

package org.apache.gobblin.multistage.filter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import org.apache.gobblin.multistage.util.JsonElementTypes;
import org.apache.gobblin.multistage.util.JsonIntermediateSchema;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@Test
public class JsonSchemaBasedFilterTest {
  private Gson gson = new Gson();
  private JsonSchemaBasedFilter JsonSchemaBasedFilter;

  @BeforeMethod
  public void Setup(){
    JsonIntermediateSchema schema = Mockito.mock(JsonIntermediateSchema.class);
    JsonSchemaBasedFilter = new JsonSchemaBasedFilter(schema);
  }

  @Test
  public void testFilterMatch() {
    JsonArray schemaJson = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream(
        "/json/schema-filter-match.json")), JsonArray.class);
    JsonArray dataJson = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream(
        "/json/schema-filter-data.json")), JsonArray.class);
    JsonIntermediateSchema schema = new JsonIntermediateSchema(schemaJson);
    JsonSchemaBasedFilter filter = new JsonSchemaBasedFilter(schema);
    JsonObject filtered = filter.filter(dataJson.get(0).getAsJsonObject());

    Assert.assertEquals(filtered, dataJson.get(0).getAsJsonObject());
  }

  @Test
  public void testFilterReduced1() {
    // Test filtering out top level column
    JsonArray schemaJson = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream(
        "/json/schema-filter-reduced1.json")), JsonArray.class);
    JsonArray dataJson = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream(
        "/json/schema-filter-data.json")), JsonArray.class);
    JsonIntermediateSchema schema = new JsonIntermediateSchema(schemaJson);
    JsonSchemaBasedFilter filter = new JsonSchemaBasedFilter(schema);
    JsonObject filtered = filter.filter(dataJson.get(0).getAsJsonObject());

    Assert.assertNotEquals(filtered, dataJson.get(0).getAsJsonObject());
  }

  @Test
  public void testFilterReduced2() {
    // Test filtering out nested column
    JsonArray schemaJson = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream(
        "/json/schema-filter-reduced2.json")), JsonArray.class);
    JsonArray dataJson = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream(
        "/json/schema-filter-data.json")), JsonArray.class);
    JsonIntermediateSchema schema = new JsonIntermediateSchema(schemaJson);
    JsonSchemaBasedFilter filter = new JsonSchemaBasedFilter(schema);
    JsonObject filtered = filter.filter(dataJson.get(0).getAsJsonObject());

    Assert.assertNotEquals(filtered, dataJson.get(0).getAsJsonObject());
  }

  /**
   * Test filter(JsonArray input) with any JsonArray object
   * Expect: a empty JsonArray instance
   */
  @Test
  public void testFilter() throws Exception {
    JsonArray inputArray = gson.fromJson("[{\"name\": \"column\"}]", JsonArray.class);;
    Method method = JsonSchemaBasedFilter.class.getDeclaredMethod("filter", JsonArray.class);
    method.setAccessible(true);
    Assert.assertEquals(method.invoke(JsonSchemaBasedFilter, inputArray), new JsonArray());
  }

  /**
   * Test filter(JsonIntermediateSchema.JisDataType dataType, JsonObject input)
   */
  @Test
  public void testFilterWithJsonObjectParameter() throws Exception {
    JsonIntermediateSchema.JisDataType jisDataType = Mockito.mock(JsonIntermediateSchema.JisDataType.class);
    Method method = JsonSchemaBasedFilter.class.getDeclaredMethod("filter", JsonIntermediateSchema.JisDataType.class, JsonObject.class);
    method.setAccessible(true);
    when(jisDataType.getType()).thenReturn(JsonElementTypes.RECORD);
    Assert.assertEquals(method.invoke(JsonSchemaBasedFilter, jisDataType, new JsonObject()), new JsonObject());

    when(jisDataType.getType()).thenReturn(JsonElementTypes.STRING);
    Assert.assertEquals(method.invoke(JsonSchemaBasedFilter, jisDataType, new JsonObject()), new JsonObject());
  }

  /**
   * Test filter(JsonIntermediateSchema.JisDataType dataType, JsonPrimitive input)
   */
  @Test
  public void testFilterWithJsonPrimitiveParameter() throws Exception {
    JsonIntermediateSchema.JisDataType jisDataType = Mockito.mock(JsonIntermediateSchema.JisDataType.class);
    Method method = JsonSchemaBasedFilter.class.getDeclaredMethod("filter", JsonIntermediateSchema.JisDataType.class, JsonPrimitive.class);
    method.setAccessible(true);
    when(jisDataType.isPrimitive()).thenReturn(true);
    Assert.assertEquals(method.invoke(JsonSchemaBasedFilter, jisDataType, new JsonPrimitive(true)), new JsonPrimitive(true));

    when(jisDataType.isPrimitive()).thenReturn(false);
    Assert.assertNull(method.invoke(JsonSchemaBasedFilter, jisDataType, new JsonPrimitive(true)));
  }
  /**
   * Test filter(JsonIntermediateSchema.JisDataType dataType, JsonElement input)
   */
  @Test
  public void testFilterWithJsonJsonElementParameter() throws Exception {
    Method method = JsonSchemaBasedFilter.class.getDeclaredMethod("filter", JsonIntermediateSchema.JisDataType.class, JsonElement.class);
    method.setAccessible(true);

    JsonIntermediateSchema.JisDataType jisDataType = Mockito.mock(JsonIntermediateSchema.JisDataType.class);
    when(jisDataType.isPrimitive()).thenReturn(false);

    JsonElement jsonElement = gson.fromJson("[]", JsonElement.class);
    when(jisDataType.getType()).thenReturn(JsonElementTypes.ARRAY);
    when(jisDataType.getItemType()).thenReturn(jisDataType);
    Assert.assertEquals(method.invoke(JsonSchemaBasedFilter, jisDataType, jsonElement), jsonElement);

    when(jisDataType.getType()).thenReturn(JsonElementTypes.OBJECT);
    Assert.assertEquals(method.invoke(JsonSchemaBasedFilter, jisDataType, jsonElement), null);
  }
}
