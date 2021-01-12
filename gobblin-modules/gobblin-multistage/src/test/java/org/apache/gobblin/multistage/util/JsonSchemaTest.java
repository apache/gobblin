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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.gobblin.multistage.util.JsonElementTypes;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link JsonSchema}
 * @author chrli
 *
 */
@Test(groups = {"org.apache.gobblin.util"})
public class JsonSchemaTest {
  private final static String originalSchema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}";
  private Gson gson;

  @BeforeClass
  public void setup() throws Exception {
    gson = new Gson();
  }

  @Test
  public void testGetAltSchema() {

    String results = "[{\"columnName\":\"metaData\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"metaData\",\"values\":[{\"columnName\":\"id\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"url\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"title\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"scheduled\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"started\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"duration\",\"isNullable\":false,\"dataType\":{\"type\":\"int\"}},{\"columnName\":\"primaryUserId\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"direction\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"system\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"scope\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"media\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"language\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]}},{\"columnName\":\"context\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"context\",\"items\":{\"name\":\"contextItem\",\"dataType\":{\"name\":\"contextItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"system\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"objects\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"objects\",\"items\":{\"name\":\"objectsItem\",\"dataType\":{\"name\":\"objectsItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"objectType\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"objectId\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"fields\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"fields\",\"items\":{\"name\":\"fieldsItem\",\"dataType\":{\"name\":\"fieldsItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"name\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"value\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]}}}}]}}}}]}}}},{\"columnName\":\"parties\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"parties\",\"items\":{\"name\":\"partiesItem\",\"dataType\":{\"name\":\"partiesItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"id\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"emailAddress\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"name\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"title\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"userId\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"speakerId\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"context\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"context\",\"items\":{\"name\":\"contextItem\",\"dataType\":{\"name\":\"contextItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"system\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"objects\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"objects\",\"items\":{\"name\":\"objectsItem\",\"dataType\":{\"name\":\"objectsItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"objectType\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"objectId\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]}}}}]}}}},{\"columnName\":\"affiliation\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"methods\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"items\":\"string\"}},{\"columnName\":\"phoneNumber\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]}}}},{\"columnName\":\"content\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"content\",\"values\":[{\"columnName\":\"trackers\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"trackers\",\"items\":{\"name\":\"trackersItem\",\"dataType\":{\"name\":\"trackersItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"name\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"count\",\"isNullable\":false,\"dataType\":{\"type\":\"int\"}},{\"columnName\":\"phrases\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"phrases\",\"items\":{\"name\":\"phrasesItem\",\"dataType\":{\"name\":\"phrasesItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"count\",\"isNullable\":false,\"dataType\":{\"type\":\"int\"}},{\"columnName\":\"phrase\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]}}}}]}}}},{\"columnName\":\"topics\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"topics\",\"items\":{\"name\":\"topicsItem\",\"dataType\":{\"name\":\"topicsItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"name\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"duration\",\"isNullable\":false,\"dataType\":{\"type\":\"int\"}}]}}}},{\"columnName\":\"structure\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"structure\",\"items\":{\"name\":\"structureItem\",\"dataType\":{\"name\":\"structureItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"name\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"duration\",\"isNullable\":false,\"dataType\":{\"type\":\"int\"}}]}}}}]}},{\"columnName\":\"interaction\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"interaction\",\"values\":[{\"columnName\":\"speakers\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"speakers\",\"items\":{\"name\":\"speakersItem\",\"dataType\":{\"name\":\"speakersItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"id\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"userId\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"talkTime\",\"isNullable\":false,\"dataType\":{\"type\":\"double\"}}]}}}},{\"columnName\":\"interactionStats\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"interactionStats\",\"items\":{\"name\":\"interactionStatsItem\",\"dataType\":{\"name\":\"interactionStatsItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"name\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"value\",\"isNullable\":false,\"dataType\":{\"type\":\"double\"}}]}}}},{\"columnName\":\"video\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"video\",\"items\":{\"name\":\"videoItem\",\"dataType\":{\"name\":\"videoItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"name\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"duration\",\"isNullable\":false,\"dataType\":{\"type\":\"double\"}}]}}}}]}},{\"columnName\":\"collaboration\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"collaboration\",\"values\":[{\"columnName\":\"publicComments\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"name\":\"publicComments\",\"items\":{\"name\":\"publicCommentsItem\",\"dataType\":{\"name\":\"publicCommentsItem\",\"type\":\"record\",\"values\":[{\"columnName\":\"id\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"audioStartTime\",\"isNullable\":false,\"dataType\":{\"type\":\"double\"}},{\"columnName\":\"audioEndTime\",\"isNullable\":false,\"dataType\":{\"type\":\"double\"}},{\"columnName\":\"commenterUserId\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"comment\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"posted\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"inReplyTo\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"duringCall\",\"isNullable\":false,\"dataType\":{\"type\":\"boolean\"}}]}}}}]}}]";

    JsonObject jsonObject = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/util/jsonschema.json")), JsonObject.class);

    Assert.assertEquals(results, new JsonSchema(jsonObject).getAltSchema(new HashMap<>(), true).toString());

    JsonObject inputJsonString = gson.fromJson("{\"type\":{\"type\": \"null\"}}", JsonObject.class);
    String expected = "[{\"columnName\":\"type\",\"isNullable\":true,\"dataType\":{\"type\":\"null\"}}]";
    Map<String, String> defaultTypes = ImmutableMap.of("type", "wrongType");
    Assert.assertEquals(new JsonSchema(inputJsonString).getAltSchema(defaultTypes, true).toString(), expected);

    expected = "[{\"columnName\":\"type\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]";
    defaultTypes = ImmutableMap.of("type", "string");
    Assert.assertEquals(new JsonSchema(inputJsonString).getAltSchema(defaultTypes, true).toString(), expected);

    expected = "[{\"columnName\":\"type\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]";
    defaultTypes = ImmutableMap.of("type", "");
    Assert.assertEquals(new JsonSchema(inputJsonString).getAltSchema(defaultTypes, true).toString(), expected);

    inputJsonString = gson.fromJson("{\"context\":{\"type\": \"NULLABLEARRAY\",\"items\":{\"type\":\"string\"}}}", JsonObject.class);
    expected = "[{\"columnName\":\"context\",\"isNullable\":true,\"dataType\":{\"type\":\"array\",\"isNullable\":\"true\",\"items\":\"string\"}}]";
    defaultTypes = ImmutableMap.of("type", "");
    Assert.assertEquals(new JsonSchema(inputJsonString).getAltSchema(defaultTypes, true).toString(), expected);
  }

  @Test
  public void testGetAltSchema2() {
    String expected = "[{\"columnName\":\"__metadata\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"__metadata\",\"values\":[{\"columnName\":\"type\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"properties\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"properties\",\"values\":[{\"columnName\":\"User\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"User\",\"values\":[{\"columnName\":\"associationuri\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]}},{\"columnName\":\"Module\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"Module\",\"values\":[{\"columnName\":\"associationuri\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]}}]}}]}}]";
    JsonObject sampleSchema = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/json/sample-json-schema.json")), JsonObject.class);
    JsonSchema schema = new JsonSchema(sampleSchema);
    Assert.assertEquals(schema.getAltSchema(new HashMap<>(), false).toString(), expected);
  }

  /**
   * This test verify the functions to build a JsonSchema object step by step
   * {
   *   "type": "object",
   *   "properties": {
   *     "id": {
   *       "type": "string"
   *     },
   *     "url": {
   *       "type": "string"
   *     }
   *   }
   * }
   */
  @Test
  public void testBuildJsonSchema() {
    JsonSchema tmpSchema;
    JsonSchema finalSchema = new JsonSchema();
    tmpSchema = new JsonSchema().addPrimitiveSchemaType(JsonElementTypes.STRING);
    finalSchema = finalSchema.addMember("id", tmpSchema);

    tmpSchema = new JsonSchema().addPrimitiveSchemaType(JsonElementTypes.STRING);
    finalSchema = finalSchema.addMember("url", tmpSchema);

    finalSchema = new JsonSchema().addChildAsObject(finalSchema);
    Assert.assertEquals(finalSchema.toString(), "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"url\":{\"type\":\"string\"}}}");
  }

  /**
   * Test: add a new property to existing schema
   * Expected: the new schema includes the property
   */
  @Test
  public void testAddObjectMember() {
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"url\":{\"type\":\"string\"}}}";
    JsonSchema jsonSchema = new JsonSchema(gson.fromJson(originalSchema, JsonObject.class));
    Assert.assertEquals(jsonSchema.addObjectMember("url", gson.fromJson("{\"type\":\"string\"}", JsonObject.class)).toString(),
        expectedSchema);
  }

  /**
   * Test: add null type to existing schema
   * Expected: the new schema includes null child
   */
  @Test
  public void testAddChildAsNull() {
    String expectedSchema = "{\"type\":\"null\",\"properties\":{\"id\":{\"type\":\"string\"}},\"items\":{\"type\":\"string\"}}";
    JsonSchema jsonSchema = new JsonSchema(gson.fromJson(originalSchema, JsonObject.class));

    Assert.assertEquals(
        jsonSchema.addChildAsNull(gson.fromJson("{\"type\":\"string\"}", JsonObject.class)).toString(),
        expectedSchema);
  }

  @Test
  public void testGetColumnDefinitions() {
    JsonSchema jsonSchema = new JsonSchema(gson.fromJson(originalSchema, JsonObject.class));
    Assert.assertEquals(jsonSchema.getColumnDefinitions().toString(), "{\"id\":{\"type\":\"string\"}}");

    jsonSchema = new JsonSchema(gson.fromJson("{\"type\":\"object\"}", JsonObject.class));
    Assert.assertEquals(jsonSchema.getColumnDefinitions().toString(), "{\"type\":\"object\"}");

    jsonSchema = new JsonSchema(gson.fromJson("{\"type\":\"array\", \"items\":[\"item1\", \"item2\"]}", JsonObject.class));
    Assert.assertEquals(jsonSchema.getColumnDefinitions().toString(), "{\"type\":\"array\",\"items\":[\"item1\",\"item2\"]}");

    jsonSchema = new JsonSchema(gson.fromJson("{\"type\":\"array\"}", JsonObject.class));
    Assert.assertEquals(jsonSchema.getColumnDefinitions().toString(), "{\"type\":\"array\"}");

    jsonSchema = new JsonSchema(gson.fromJson("{\"sometype\":\"object\"}", JsonObject.class));
    Assert.assertEquals(jsonSchema.getColumnDefinitions().toString(), "{\"sometype\":\"object\"}");
  }

  /**
   * Test: equals and hashCode
   */
  @Test
  public void testHashCodeAndEquals() {
    JsonSchema jsonSchema1 = new JsonSchema(gson.fromJson(originalSchema, JsonObject.class));
    JsonSchema jsonSchema2 = new JsonSchema(gson.fromJson(originalSchema, JsonObject.class));

    Assert.assertFalse(jsonSchema1.equals(new JsonObject()));
    Assert.assertEquals(jsonSchema1.hashCode(), jsonSchema2.hashCode());
    Assert.assertEquals(jsonSchema1, jsonSchema2);
  }
}