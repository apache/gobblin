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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


/**
 * This utility class helps parse Json data and infer schema.
 *
 * Json data have a very loose schema definition, data elements can have incomplete structure from record
 * to record. In order properly infer a complete schema, a batch of records is necessary.
 *
 * TODO: to be able to parse a stream of Json records because event a batch of records sometimes
 * TODO: are insufficient.
 *
 */
public class JsonSchemaGenerator {
  final private static Logger LOGGER = Logger.getLogger(JsonSchemaGenerator.class);
  private JsonSchema schema = new JsonSchema();
  private JsonElement data;
  private boolean pivoted = false;

  public JsonSchemaGenerator(JsonElement data) {
    this.data = data;
  }

  public JsonSchemaGenerator(JsonElement data, boolean pivoted) {
    this.data = data;
    this.pivoted = pivoted;
  }

  public JsonSchema getSchema() {
    if (data.isJsonObject()) {
      schema.addChildAsObject(new JsonObject());
      for (Map.Entry<String, JsonElement> member : data.getAsJsonObject().entrySet()) {
        schema.addObjectMember(member.getKey(), inferSchema(member.getValue()));
      }
      return schema;
    }

    if (data.isJsonPrimitive() || data.isJsonNull()) {
      schema = inferSchema(data);
      return schema;
    }

    if (!pivoted) {
      schema = inferSchema(pivotJsonArray(data.getAsJsonArray()));
      return schema;
    }

    // Array of primitives or empty array
    schema = new JsonSchema();
    if (data.getAsJsonArray().size() == 0)  {
      schema.addChildAsArray(new JsonSchemaGenerator(new JsonNull()).getSchema());
    } else {
      schema.addChildAsArray(new JsonSchemaGenerator(data.getAsJsonArray().get(0)).getSchema());
    }
    return schema;
  }

  /***
   * infers schema structure out of a JSON document
   *
   * @param data a JSON document
   * @return a JsonSchema object describing the JSON document
   */
  private JsonSchema inferSchema(JsonElement data) {
    if (data.isJsonPrimitive() || data.isJsonNull()) {
      JsonObject jsonObject = new JsonObject();

      if (data.isJsonNull()) {
        jsonObject.addProperty("type", "null");
      } else if (data.toString().matches("^\".*\"$")) {
        jsonObject.addProperty("type", "string");
      } else {
        String value = data.getAsString();
        if (value.toLowerCase().matches("(true|false)")) {
          jsonObject.addProperty("type", "boolean");
        } else if (Ints.tryParse(value) != null) {
          jsonObject.addProperty("type", "integer");
        } else {
          jsonObject.addProperty("type", "number");
        }
      }
      return new JsonSchema(jsonObject);
    } else if (data.isJsonArray()
        && data.getAsJsonArray().size() > 0
        && data.getAsJsonArray().get(0).isJsonArray()) {
      /**
       * for pivoted array or arrays, we will process each sub-array as a column
       * column schema would have values like:
       *   {"type":"object","properties":{"id":{"type":"string"}}}
       */
      JsonObject jsonObject = new JsonObject();
      for (JsonElement element: data.getAsJsonArray()) {
        JsonSchema column = inferSchemaFromMultiple(element.getAsJsonArray());
        Map.Entry<String, JsonElement> entry = column.getSchema()
            .entrySet()
            .iterator()
            .next();
        jsonObject.add(entry.getKey(), entry.getValue());
      }
      return schema.addChildAsArray(jsonObject);
    } else {
      return new JsonSchemaGenerator(data, true).getSchema();
    }
  }

  /**
   * This function takes an array of the same column and infer a comprehensive schema based on all values.
   *
   * Sample value of schema 1 and schema 2 before merge:
   * {"type":"object","properties":{"id":{"type":"string"}}}
   * {"type":"object","properties":{"id":{"type":"null"}}}
   *
   * after merge, it would be
   * {"type":"object","properties":{"id":{"type": ["string", "null"]}}}
   *
   * @param data an array of data from the same column
   * @return a JsonSchema object describing the column
   */
  private JsonSchema inferSchemaFromMultiple(JsonArray data) {
    if (data.size() == 0) {
      return new JsonSchema().addMember("type", JsonNull.INSTANCE);
    }

    JsonElementTypes itemType = JsonElementTypes.getTypeFromMultiple(data);

    if (itemType.isObject()) {
      return inferObjectSchemaFromMultiple(data);
    } else if (itemType.isArray()) {
      return inferArraySchemaFromMultiple(data);
    } else {
      return new JsonSchema().addPrimitiveSchemaType(itemType);
    }
  }

  /**
   * This function takes an array of rows and infer the row schema column by column
   *
   * @param data an array of rows with 1 or more columns
   * @return the inferred Json schema
   */
  private JsonSchema inferArraySchemaFromMultiple(JsonArray data) {
    /**
     * strip off one layer of array because data is like
     * [[{something}],[{something}]]
     */
    JsonArray arrayData = new JsonArray();
    for (JsonElement element: data) {
      if (element.isJsonNull() || element.getAsJsonArray().size() == 0) {
        arrayData.add(JsonNull.INSTANCE);
      } else {
        arrayData.addAll(element.getAsJsonArray());
      }
    }
    JsonElementTypes subType = JsonElementTypes.getTypeFromMultiple(arrayData);

    if (subType.isObject()) {
      return new JsonSchema().addChildAsArray(
          new JsonSchema().addChildAsObject(
              inferSchemaOfSubTable(arrayData).get("items")));
    } else {
      // need to process array of objects
      return new JsonSchema().addChildAsArray(
          new JsonSchema().addPrimitiveSchemaType(subType));
    }
  }

  /**
   * This function takes an array of Json objects infer their schema
   *
   * @param data A Json array of objects
   * @return inferred schema
   */
  private JsonSchema inferObjectSchemaFromMultiple(JsonArray data) {

    /**
     * ignore potentially null values at the beginning
     */
    int i = 0;
    while (i < data.size() && (isEmpty(data.get(i))
        || isEmpty(data.get(i).getAsJsonObject().entrySet().iterator().next().getValue()))) {
      ++i;
    };

    /**
     * for placeholder type of fields, all values will be null, i will be larger than size
     * in this case, we just reset i to 0
     */
    if (i >= data.size()) {
      i = 0;
    }
    Map.Entry<String, JsonElement> dataEntry = data.get(i).getAsJsonObject().entrySet().iterator().next();
    String memberKey = dataEntry.getKey();
    JsonArray objectData = getValueArray(data);

    /**
     * for sub tables, need to convert to object column format
     * because objectData is an array of objects, and inferSchemaOfSubTable will
     * return an array schema
     *
     * Sub-table schema should be like following:
     *     "settings": {
     *       "type": "object",
     *       "properties": {
     *         "webConferencesRecorded": {
     *           "type": "boolean"
     *         },
     *         "preventWebConferenceRecording": {
     *           "type": "boolean"
     *         },
     *         "telephonyCallsImported": {
     *           "type": "boolean"
     *         },
     *         "emailsImported": {
     *           "type": "boolean"
     *         },
     *         "preventEmailImport": {
     *           "type": "boolean"
     *         }
     *       }
     *     }
     */
    if (isSubTable(dataEntry.getValue())) {
      return new JsonSchema().addMember(memberKey,
          new JsonSchema().addChildAsObject(
              inferSchemaOfSubTable(objectData).get("items")));
    }

    JsonElementTypes subType = JsonElementTypes.getTypeFromMultiple(objectData);

    /**
     * For columns with an array of values, such as {"aliases": ["12345"]}
     */
    if (subType.isArray()) {
      return new JsonSchema().addMember(memberKey,
          new JsonSchema().addChildAsArray(inferSchemaFromMultiple(objectData).get("items")));
    }

    return JsonSchema.buildColumnSchema(memberKey, subType);
  }

  /**
   * This function takes only the value part of the key value pair array
   *
   * @param kvArray an array of KV pairs
   * @return an array contains only the value part
   */
  JsonArray getValueArray(JsonArray kvArray) {
    int i = 0;
    while (kvArray.get(i).isJsonNull()) {
      ++i;
    }
    String key = kvArray.get(i).getAsJsonObject().entrySet().iterator().next().getKey();
    JsonArray valueArray = new JsonArray();
    for (JsonElement element: kvArray) {
      if (element.isJsonNull()) {
        valueArray.add(JsonNull.INSTANCE);
      } else {
        valueArray.add(element.getAsJsonObject().get(key));
      }
    }
    return valueArray;
  }

  /**
   * Pivot JsonArray so that all values of the same column can be parsed altogether.
   * This is important for nullability analysis. By taking only 1 record from an JsonArray
   * to derive schema for the whole dataset, we would be seeing part of the types of a nullable
   * column.
   *
   * The input can be:
   *   1. array of JsonObjects
   *   2. array of Primitives
   *   3. array of Arrays
   *
   * The input cannot be:
   *   4. array of mixed types
   *
   * TODO: to handle union types, this requires further work
   *
   * @param data a JsonArray of records
   * @return an JsonArray of JsonArrays
   */
  JsonArray pivotJsonArray(JsonArray data) {
    int i = 0;
    JsonArray pivotedArray = new JsonArray();
    Map<String, Integer> columnIndex = new HashMap<>();

    while (i < data.size()
        && (data.get(i).isJsonNull() || isEmpty(data.get(i)))) {
      ++i;
    }

    /**
     * in case data has no records, or data has only blank records, then no action and
     * return a blank pivoted array.
     */
    if (i >= data.size()) {
      return pivotedArray;
    }

    JsonElementTypes elementType = getJsonElementType(data.get(i));

    if (elementType == JsonElementTypes.PRIMITIVE) {
      return data;
    }

    for (JsonElement row: data) {
      if (!row.isJsonObject()) {
        LOGGER.error("Array of Arrays is not supported");
        return new JsonArray();
      }
      for (Map.Entry<String, JsonElement> entry : row.getAsJsonObject().entrySet()) {
        if (!columnIndex.containsKey(entry.getKey())) {
          pivotedArray.add(new JsonArray());
          columnIndex.put(entry.getKey(), columnIndex.size());
        }
      }
    }

    for (JsonElement element: data) {
      if (element.isJsonNull() || isEmpty(element)) {
        for (i = 0; i < columnIndex.size(); ++i) {
          pivotedArray.get(i).getAsJsonArray().add(JsonNull.INSTANCE);
        }
      } else {
        /**
         * each element might have columns in different order,
         * and all elements don't have the same columns
         */
        Preconditions.checkState(elementType == JsonElementTypes.OBJECT);
        for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
          JsonObject temp = new JsonObject();
          temp.add(entry.getKey(), entry.getValue());
          if (columnIndex.get(entry.getKey()) != null && pivotedArray.size() > columnIndex.get(entry.getKey())) {
            pivotedArray.get(columnIndex.get(entry.getKey())).getAsJsonArray().add(temp);
          } else {
            pivotedArray.add(new JsonArray());
            columnIndex.put(entry.getKey(), columnIndex.size());
          }
        }
      }
    }
    return pivotedArray;
  }

  boolean isSubTable(JsonElement data) {
    return data.isJsonObject() && data.getAsJsonObject().entrySet().size() > 0;
  }

  JsonSchema inferSchemaOfSubTable(JsonArray data) {
    /**
     * This is for sub-tables, where a column contains a structure like
     * {"settings": {"option1":false,"option2":false}
     */
    return new JsonSchemaGenerator(data).getSchema();
  }

  /**
   * Classifies an Json element to 4 high level data types, but doesn't identify further
   * detailed types of primitives
   *
   * @param jsonElement a Json element
   * @return ARRAY, OBJECT, NULL, or PRIMITIVE
   */
  JsonElementTypes getJsonElementType(JsonElement jsonElement) {
    if (jsonElement.isJsonPrimitive()) {
      return JsonElementTypes.PRIMITIVE;
    } else if (jsonElement.isJsonNull()) {
      return JsonElementTypes.NULL;
    } else if (jsonElement.isJsonObject()) {
      return JsonElementTypes.OBJECT;
    } else {
      return JsonElementTypes.ARRAY;
    }
  }

  /**
   * in real world Json strings, empty element can be presented in different forms
   *
   * @param data input data to test
   * @return if data represent an empty object
   *
   */
  boolean isEmpty(JsonElement data) {
    if (data == null
        || data.isJsonNull()
        || (data.isJsonObject() && data.toString().equals("{}"))
        || (data.isJsonArray() && data.toString().equals("[]"))
        || (data.isJsonPrimitive() && StringUtils.isEmpty(data.getAsString()))) {
      return true;
    }
    return false;
  }
}
