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
package org.apache.gobblin.converter.json;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import gobblin.configuration.WorkUnitState;

import static org.junit.Assert.assertEquals;


/**
 * Unit test for {@link JsonStringToJsonIntermediateConverter}
 *
 * @author Tilak Patidar
 */
@Test(groups = {"gobblin.converter"})
public class JsonStringToJsonIntermediateConverterTest {

  private static JsonStringToJsonIntermediateConverter converter;
  private static JsonObject testJsonData;

  @BeforeClass
  public static void setUp()
      throws SchemaConversionException {
    converter = new JsonStringToJsonIntermediateConverter();
    WorkUnitState workUnit = new WorkUnitState();
    workUnit.getPropAsBoolean("gobblin.converter.jsonStringToJsonIntermediate.unpackComplexSchemas", true);
    converter.convertSchema("[]", workUnit);
    Type jsonType = new TypeToken<JsonObject>() {
    }.getType();
    Gson gson = new Gson();
    testJsonData = gson.fromJson(new InputStreamReader(JsonStringToJsonIntermediateConverterTest.class
        .getResourceAsStream("/converter/JsonStringToJsonIntermediateConverter.json")), jsonType);
  }

  private JsonObject parseJsonObject(JsonObject json, JsonArray record)
      throws DataConversionException {
    return converter.convertRecord(record, json.toString(), new WorkUnitState()).iterator().next();
  }

  @Test
  public void testAllCases()
      throws DataConversionException {
    for (Map.Entry<String, JsonElement> keyset : testJsonData.entrySet()) {
      JsonArray testData = keyset.getValue().getAsJsonArray();
      JsonObject json = testData.get(0).getAsJsonObject();
      JsonArray schema = testData.get(1).getAsJsonArray();
      JsonObject expected = testData.get(2).getAsJsonObject();
      JsonObject result = null;
      try {
        result = parseJsonObject(json, schema);
      } catch (Exception e) {
        e.printStackTrace();
        assertEquals("Test case failed : " + keyset.getKey(), "No exception", e.getMessage());
      }
      assertEquals("Test case failed : " + keyset.getKey(), expected, result);
    }
  }

  @Test(expectedExceptions = DataConversionException.class, expectedExceptionsMessageRegExp = "Invalid symbol.*")
  public void jsonWithInvalidEnumEntry()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":\"TROLL\"}";
    String schemaStr =
        "    [{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"enum\", \"symbols\":[\"HELL\",\"BELLS\"]}}]";

    parseJsonObject(buildJsonObject(jsonStr), buildJsonArray(schemaStr));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "Array items can only be defined using JsonObject or JsonPrimitive.")
  public void jsonWithArrayOfMapContainingRecordWithWrongSchema()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":[{\"d\":{\"age\":\"10\"}},{\"d\":{\"age\":\"1\"}}]}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":[{\"dataType\":{\"type\":\"map\", \"values\":{\"dataType\":{\"type\":\"record\",\"values\":[{\"columnName\":\"age\", \"dataType\":{\"type\":\"int\"}}]}}}}]}}]";

    parseJsonObject(buildJsonObject(jsonStr), buildJsonArray(schemaStr));
  }

  private JsonObject buildJsonObject(String s) {
    JsonParser parser = new JsonParser();
    return (JsonObject) parser.parse(s);
  }

  private JsonArray buildJsonArray(String schemaStr) {
    JsonParser parser = new JsonParser();
    return parser.parse(schemaStr).getAsJsonArray();
  }
}