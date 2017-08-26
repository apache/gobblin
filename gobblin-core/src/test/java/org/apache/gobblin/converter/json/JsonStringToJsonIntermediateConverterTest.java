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

import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.configuration.WorkUnitState;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link JsonStringToJsonIntermediateConverter}
 *
 * @author Tilak Patidar
 */
public class JsonStringToJsonIntermediateConverterTest {

  private static JsonStringToJsonIntermediateConverter converter;

  @BeforeClass
  public static void setUp()
      throws SchemaConversionException {
    converter = new JsonStringToJsonIntermediateConverter();
    WorkUnitState workUnit = new WorkUnitState();
    workUnit.getPropAsBoolean("gobblin.converter.jsonStringToJsonIntermediate.unpackComplexSchemas", true);
    converter.convertSchema("[]", workUnit);
  }

  private JsonObject parseJsonObject(String jsonStr, JsonArray record)
      throws DataConversionException {
    return converter.convertRecord(record, jsonStr, new WorkUnitState()).iterator().next();
  }

  @Test
  public void emptyJson()
      throws DataConversionException {
    String jsonStr = "{}";
    String schemaStr = "[]";
    JsonObject expected = buildJsonObject("{}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithNullValue()
      throws DataConversionException {
    String jsonStr = "{\"a\":null}";
    String schemaStr = "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"null\"}}]";
    JsonObject expected = buildJsonObject("{\"a\":null}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithFloat()
      throws DataConversionException {
    String jsonStr = "{\"a\":0.8}";
    String schemaStr = "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"float\"}}]";
    JsonObject expected = buildJsonObject("{\"a\":0.8}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithBytes()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"\\u00FF\"}";
    String schemaStr = "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"bytes\"}}]";
    JsonObject expected = buildJsonObject("{\"a\":\"\\u00FF\"}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithExtraFieldsThanSchema()
      throws DataConversionException {
    String jsonStr = "{\"a\":1, \"b\":6}";
    String schemaStr = "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"int\"}}]";
    JsonObject expected = buildJsonObject("{\"a\":1}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithCompleteFieldsInSchema()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":6}";
    String schemaStr = "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}}]";
    JsonObject expected = buildJsonObject("{\"a\":\"somename\"}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithBooleanValue()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":true}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"boolean\"}}]";
    JsonObject expected = buildJsonObject("{\"a\":\"somename\", \"b\":true}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithArrayOfInts()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":[1,2,3]}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":\"int\"}}]";
    JsonObject expected = buildJsonObject("{\"a\":\"somename\", \"b\":[1,2,3]}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithSingleKVMapAsValue()
      throws DataConversionException {
    String jsonStr = "{\"a\":{\"b\":\"somename\"}}";
    String schemaStr = "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"map\", \"values\":\"string\"}}]";
    JsonObject expected = buildJsonObject("{\"a\":{\"b\":\"somename\"}}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);
    assertEquals(expected, result);
  }

  @Test
  public void jsonWithTwoKVMap()
      throws DataConversionException {
    String jsonStr = "{\"a\":{\"b\":\"somename\",\"count\":6}}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"record\", \"values\":[{\"columnName\":\"b\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"count\",\"dataType\":{\"type\":\"int\"}}]}}]";
    JsonObject expected = buildJsonObject("{\"a\":{\"b\":\"somename\",\"count\":6}}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);
    assertEquals(expected, result);
  }

  @Test
  public void jsonWithArrayOfSingleKVMap()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":[{\"c\":\"1\"},{\"d\":\"1\"}]}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":{\"dataType\":{\"type\":\"map\", \"values\":\"string\"}}}}]";
    JsonObject expected = buildJsonObject("{\"a\":\"somename\", \"b\":[{\"c\":\"1\"},{\"d\":\"1\"}]}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithArrayOfTwoRecords()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":[{\"name\": \"me\", \"c\":1},{\"name\": \"me\", \"c\":1}]}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":{\"dataType\":{\"type\":\"record\", \"values\":[{\"columnName\": \"name\", \"dataType\":{\"type\":\"string\"}},{\"columnName\": \"c\", \"dataType\":{\"type\":\"long\"}}]}}}}]";
    JsonObject expected =
        buildJsonObject("{\"a\":\"somename\", \"b\":[{\"name\": \"me\", \"c\":1},{\"name\": \"me\", \"c\":1}]}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithArrayOfTwoMap()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":[{\"d\":\"1\"},{\"d\":\"1\"}]}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":{\"dataType\":{\"type\":\"map\", \"values\":\"string\"}}}}]";
    buildJsonObject("{\"a\":\"somename\", \"b\":[{\"c\":\"1\", \"e\":1},{\"d\":\"1\"}]}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject expected = buildJsonObject("{\"a\":\"somename\", \"b\":[{\"d\":\"1\"},{\"d\":\"1\"}]}");

    JsonObject result = parseJsonObject(jsonStr, record);
    assertEquals(expected, result);
  }

  @Test
  public void jsonWithRecord()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":{\"c\":\"1\", \"d\":1}}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"record\", \"values\":[{\"columnName\":\"c\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"d\",\"dataType\":{\"type\":\"int\"}}]}}]";
    JsonObject expected = buildJsonObject("{\"a\":\"somename\", \"b\":{\"c\":\"1\", \"d\":1}}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithRecordInSchemaButNotInData()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":{}}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"record\", \"values\":[{\"columnName\":\"c\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"d\",\"dataType\":{\"type\":\"int\"}}]}}]";
    JsonObject expected = buildJsonObject("{\"a\":\"somename\", \"b\":{\"c\":null,\"d\":null}}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithFixedType()
      throws DataConversionException {
    String jsonStr = "{\"a\":1, \"b\":\"hello\"}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"int\"}}, {\"columnName\":\"b\", \"dataType\":{\"type\":\"fixed\", \"size\": 5, \"name\":\"otp\"}}]";
    JsonObject expected = buildJsonObject("{\"a\":1, \"b\":\"hello\"}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test
  public void jsonWithEnums()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":\"HELL\"}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"enum\", \"symbols\":[\"HELL\",\"BELLS\"]}}]";
    JsonObject expected = buildJsonObject("{\"a\":\"somename\", \"b\":\"HELL\"}");
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    JsonObject result = parseJsonObject(jsonStr, record);

    assertEquals(expected, result);
  }

  @Test(expected = DataConversionException.class)
  public void jsonWithInvalidEnumEntry()
      throws DataConversionException {
    String jsonStr = "{\"a\":\"somename\", \"b\":\"TROLL\"}";
    String schemaStr =
        "[{\"columnName\":\"a\", \"dataType\":{\"type\":\"string\"}},{\"columnName\":\"b\", \"dataType\":{\"type\":\"enum\", \"symbols\":[\"HELL\",\"BELLS\"]}}]";

    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    parseJsonObject(jsonStr, record);
  }

  @Test
  public void jsonWithMapOfRecords()
      throws Exception {
    String jsonStr =
        "{\"persons\":{\"someperson\":{\"name\":\"someone\", \"age\": 11}, \"otherperson\":{\"name\":\"someoneelse\", \"age\": 12}}}";
    String schemaStr =
        "[{\"columnName\":\"persons\", \"dataType\": {\"type\":\"map\", \"values\":{\"dataType\":{\"type\":\"record\",\"values\":[{\"columnName\":\"name\", \"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"age\", \"dataType\":{\"type\":\"int\"}}]}}}}]";
    String expected =
        "{\"persons\":{\"someperson\":{\"name\":\"someone\",\"age\":11},\"otherperson\":{\"name\":\"someoneelse\",\"age\":12}}}";
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    assertEquals(expected, parseJsonObject(jsonStr, record).toString());
  }

  @Test
  public void jsonWithMapOfArray()
      throws Exception {
    String jsonStr = "{\"persons\":{\"someperson\":[10,20], \"otherperson\": [20,50]}}";
    String schemaStr =
        "[{\"columnName\":\"persons\", \"dataType\": {\"type\":\"map\", \"values\":{\"dataType\":{\"type\":\"array\",\"items\":\"int\"}}}}]";
    String expected = "{\"persons\":{\"someperson\":[10,20],\"otherperson\":[20,50]}}";
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    assertEquals(expected, parseJsonObject(jsonStr, record).toString());
  }

  @Test
  public void jsonWithMapOfEnum()
      throws Exception {
    String jsonStr = "{\"persons\":{\"someperson\":\"YES\", \"otherperson\": \"NO\"}}";
    String schemaStr =
        "[{\"columnName\":\"persons\", \"dataType\": {\"type\":\"map\", \"values\":{\"dataType\":{\"name\":\"choice\", \"type\":\"enum\",\"symbols\":[\"YES\",\"NO\"]}}}}]";
    String expected = "{\"persons\":{\"someperson\":\"YES\",\"otherperson\":\"NO\"}}";
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    assertEquals(expected, parseJsonObject(jsonStr, record).toString());
  }

  @Test
  public void jsonWithRecordContainingArray()
      throws Exception {
    String jsonStr = "{\"persons\":{\"someperson\":[10,20]}}";
    String schemaStr =
        "[{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"type\":\"array\",\"items\":\"int\"}}]}}]";
    String expected = "{\"persons\":{\"someperson\":[10,20]}}";
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    String actual = parseJsonObject(jsonStr, record).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void jsonWithRecordContainingEnums()
      throws Exception {
    String jsonStr = "{\"persons\":{\"someperson\":\"YES\"}}";
    String schemaStr =
        "[{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"name\": \"choice\", \"type\":\"enum\",\"symbols\":[\"YES\", \"NO\"]}}]}}]";
    String expected = "{\"persons\":{\"someperson\":\"YES\"}}";
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    String actual = parseJsonObject(jsonStr, record).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void jsonWithRecordContainingMap()
      throws Exception {
    String jsonStr = "{\"persons\":{\"someperson\":{\"1\":\"2\"}}}";
    String schemaStr =
        "[{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"type\":\"map\",\"values\":\"string\"}}]}}]";
    String expected = "{\"persons\":{\"someperson\":{\"1\":\"2\"}}}";
    JsonParser parser = new JsonParser();
    JsonArray record = parser.parse(schemaStr).getAsJsonArray();

    String actual = parseJsonObject(jsonStr, record).toString();
    assertEquals(expected, actual);
  }

  private JsonObject buildJsonObject(String s) {
    JsonParser parser = new JsonParser();
    return (JsonObject) parser.parse(s);
  }
}