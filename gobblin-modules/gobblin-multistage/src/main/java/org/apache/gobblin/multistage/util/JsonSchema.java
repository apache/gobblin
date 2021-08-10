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
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;


/**
 * This class is defined so that we can get a Json Schema as close to JSON Schema standards as possible (draft-07).
 * However, an alternative schema format, so referenced as Json Intermediate, can be enabled.
 *
 * Other JsonSchema classes (several!) defined in Gobblin framework more or less resembles an Avro Schema.
 *
 * A sample different in defining a nullable field "email":
 *
 * Json schema: {"name": {"type": ["string", "null"]}}
 *
 * Json intermediate schema: {"columnName": "name", "isNullable": "true", "dateType": {"type": "string"}}
 *
 */
@Slf4j
public class JsonSchema {
  final private static String KEY_WORD_ARRAY = "array";
  final private static String KEY_WORD_COLUMN_NAME = "columnName";
  final private static String KEY_WORD_DATA_TYPE = "dataType";
  final private static String KEY_WORD_ITEMS = "items";
  final private static String KEY_WORD_NAME = "name";
  final private static String KEY_WORD_OBJECT = "object";
  final private static String KEY_WORD_PROPERTIES = "properties";
  final private static String KEY_WORD_TYPE = "type";
  final private static String KEY_WORD_VALUES = "values";
  private JsonObject schema;
  Gson gson = new Gson();

  public JsonSchema() {
    this.schema = new JsonObject();
  }

  public JsonSchema(JsonObject schema) {
    this.schema = schema;
  }

  public JsonSchema addChildAsObject(JsonSchema other) {
    schema.addProperty(KEY_WORD_TYPE, KEY_WORD_OBJECT);
    schema.add(KEY_WORD_PROPERTIES, other.schema);
    return this;
  }

  public JsonSchema addChildAsObject(JsonObject jsonObject) {
    schema.addProperty(KEY_WORD_TYPE, KEY_WORD_OBJECT);
    schema.add(KEY_WORD_PROPERTIES, jsonObject);
    return this;
  }

  public JsonSchema addObjectMember(String member, JsonObject property) {
    schema.get(KEY_WORD_PROPERTIES).getAsJsonObject().add(member, property);
    return this;
  }

  public JsonSchema addObjectMember(String member, JsonSchema property) {
    schema.get(KEY_WORD_PROPERTIES).getAsJsonObject().add(member, property.schema);
    return this;
  }

  public JsonSchema addProperty(String propertyName, String propertyValue) {
    schema.addProperty(propertyName, propertyValue);
    return this;
  }

  public JsonSchema addChildAsArray(JsonSchema other) {
    schema.addProperty(KEY_WORD_TYPE, KEY_WORD_ARRAY);
    schema.add(KEY_WORD_ITEMS, other.schema);
    return this;
  }

  public JsonSchema addChildAsArray(JsonObject jsonObject) {
    schema.addProperty(KEY_WORD_TYPE, KEY_WORD_ARRAY);
    schema.add(KEY_WORD_ITEMS, jsonObject);
    return this;
  }

  public JsonSchema addMember(String member, JsonElement value) {
    schema.add(member, value);
    return this;
  }

  public JsonSchema addMember(String member, JsonSchema value) {
    schema.add(member, value.getSchema());
    return this;
  }
  public JsonSchema addChildAsNull(JsonObject jsonObject) {
    schema.addProperty(KEY_WORD_TYPE, "null");
    schema.add(KEY_WORD_ITEMS, jsonObject);
    return this;
  }

  /**
   * Add a type description to the schema element
   *
   * Primitive schema item types can have values like following:
   * {"type", "null"}
   * {"type", "string"}
   * {"type", ["string", "null"]}
   *
   * @param itemType the type of this JsonSchema item
   * @return the modified JsonSchema item with the specified type
   */
  public JsonSchema addPrimitiveSchemaType(JsonElementTypes itemType) {
    if (itemType.isNull()) {
      return this.addProperty(KEY_WORD_TYPE, "null");
    }

    if (itemType.isNullable()) {
      JsonArray typeArray = new JsonArray();
      typeArray.add(itemType.reverseNullability().toString());
      typeArray.add("null");
      this.addMember(KEY_WORD_TYPE, typeArray);
    } else {
      this.addProperty(KEY_WORD_TYPE, itemType.toString());
    }

    return this;
  }

  /**
   *
   * @return return a copy of the private schema object
   */
  public JsonObject getSchema() {
    return JsonUtils.deepCopy(this.schema).getAsJsonObject();
  }

  public JsonObject get(String key) {
    return this.schema.get(key).getAsJsonObject();
  }

  /**
   * an array has column definition in "items"
   * an object has column definition in "properties"
   *
   * @return the column definitions of the schema
   */
  public JsonObject getColumnDefinitions() {
    JsonObject temp = JsonUtils.deepCopy(schema).getAsJsonObject();
    try {
      // a schema might contain a column called type, in such case,
      // the value of the "type" element should be non-primitive
      // and following statement will throw an UnsupportedOperation exception
      String schemaForm = temp.get(KEY_WORD_TYPE).getAsString();
      if (schemaForm.equalsIgnoreCase(KEY_WORD_OBJECT) && temp.has(KEY_WORD_PROPERTIES)) {
        temp = temp.get(KEY_WORD_PROPERTIES).getAsJsonObject();
      } else if (schemaForm.equalsIgnoreCase(KEY_WORD_ARRAY) && temp.has(KEY_WORD_ITEMS)) {
        temp = temp.get(KEY_WORD_ITEMS).getAsJsonObject();
      }
    } finally {
      return temp;
    }
  }

  public String toString() {
    return this.schema.toString();
  }

  @Override
  public int hashCode() {
    return schema.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof JsonSchema) {
      return this.schema.equals(((JsonSchema) other).schema);
    }
    return false;
  }

  /**
   * Sample schema after conversion
   * {
   *   "columnName": "settings",
   *   "dataType": {
   *     "type": "record",
   *     "fields": [{
   *       "columnName": "webConferencesRecorded",
   *       "dataType": {
   *         "type": "boolean"
   *       }
   *     }, {
   *       "columnName": "preventWebConferenceRecording",
   *       "dataType": {
   *         "type": "boolean"
   *       }
   *     }, {
   *       "columnName": "telephonyCallsImported",
   *       "dataType": {
   *         "type": "boolean"
   *       }
   *     }, {
   *       "columnName": "emailsImported",
   *       "dataType": {
   *         "type": "boolean"
   *       }
   *     }, {
   *       "columnName": "preventEmailImport",
   *       "dataType": {
   *         "type": "boolean"
   *       }
   *     }]
   *   }
   * }
   *
   * @param columnName the column name key
   * @param jsonColumn the column definition in Json Schema format
   * @return converted alternate format
   */
  private JsonObject getAltColumnSchema(String columnName,
      JsonObject jsonColumn,
      String defaultType,
      boolean enableCleansing) {

    JsonObject convertedColumn = new JsonSchema().addProperty(KEY_WORD_COLUMN_NAME, columnName).getSchema();

    JsonElementTypes columnType = JsonElementTypes.forType(jsonColumn.get(KEY_WORD_TYPE).toString());

    /**
     * TODO: add default all Nullable and exclude only those specified in configuration
     */
    convertedColumn.addProperty("isNullable", columnType.isNullable());

    if (columnType.isNull()) {
      if (defaultType != null && !defaultType.isEmpty()) {
        try {
          columnType = Enum.valueOf(JsonElementTypes.class, defaultType.toUpperCase());
        } catch (Exception e) {
          log.info("Incorrect default data type {} for {}, this column is assumed to be of String type.", defaultType, columnName);
        }
      } else {
        columnType = JsonElementTypes.STRING;
      }
    }

    JsonObject dataType = new JsonSchema().addProperty(KEY_WORD_TYPE, columnType.getAltName()).getSchema();

    if (columnType.getAltName().equals(JsonElementTypes.ARRAY.getAltName())) {
      JsonElementTypes subType = JsonElementTypes.forType(jsonColumn
          .get(KEY_WORD_ITEMS).getAsJsonObject()
          .get(KEY_WORD_TYPE).toString());

      if (columnType.isNullable()
          || columnType.equals(JsonElementTypes.ARRAY)
          || columnType.equals(JsonElementTypes.OBJECT)) {
        dataType.addProperty("isNullable", "true");
      }

      if (subType.isObject()) {
        dataType.addProperty(KEY_WORD_NAME, columnName);
        dataType.add(KEY_WORD_ITEMS,
            new JsonSchema().addProperty(KEY_WORD_NAME, columnName + "Item")
                .addMember(KEY_WORD_DATA_TYPE,
                new JsonSchema().addProperty(KEY_WORD_NAME, columnName + "Item")
                    .addProperty(KEY_WORD_TYPE, subType.getAltName())
                    .addMember(KEY_WORD_VALUES,
                        new JsonSchema(jsonColumn.get(KEY_WORD_ITEMS).getAsJsonObject().get(KEY_WORD_PROPERTIES).getAsJsonObject())
                            .getAltSchema(new HashMap<>(), enableCleansing))).getSchema());
      } else {
        dataType.addProperty(KEY_WORD_ITEMS, subType.isNull() ? defaultType : subType.getAltName());
      }

    } else if (columnType.getAltName().equals(JsonElementTypes.OBJECT.getAltName())) {
      dataType.addProperty(KEY_WORD_NAME, columnName);
      dataType.add(KEY_WORD_VALUES, new JsonSchema(jsonColumn
          .get(KEY_WORD_PROPERTIES)
          .getAsJsonObject())
          .getAltSchema(new HashMap<>(), enableCleansing));
    }

    convertedColumn.add(KEY_WORD_DATA_TYPE, dataType);
    return convertedColumn;
  }

  public JsonArray getAltSchema(Map<String, String> defaultTypes,
      boolean enableCleansing) {
    JsonArray jsonArray = new JsonArray();
    for (Map.Entry<String, JsonElement> column : getColumnDefinitions().entrySet()) {
      // a column definition should be in the format of: <<column name>> : {"type": <<column type>>}
      // if not, we should assume it is a dummy column
      if (column.getValue().isJsonObject()) {
        jsonArray.add(getAltColumnSchema(enableCleansing
                ? column.getKey().replaceAll("(\\s|\\$)", "_")
                : column.getKey(),
            column.getValue().getAsJsonObject(),
            defaultTypes.get(column.getKey()), enableCleansing));
      } else {
        // this can be a dummy column or a non-conforming JsonSchema
        // if it is non-conforming, string type assumed
        JsonObject columnType = new JsonObject();
        columnType.addProperty("type",
            column.getKey().equalsIgnoreCase("type")
                ? column.getValue().getAsString() : "string");
        jsonArray.add(getAltColumnSchema("dummy", columnType, null, false));
      }
    }
    return jsonArray;
  }

  /**
   * when the member value is primitive or null
   * column schema object can have values like {"id": "12345"} or {"email": null}
   * @param name column name
   * @param type the JsonElementType, including nullability
   * @return a column schema object
   */
  public static JsonSchema buildColumnSchema(String name, JsonElementTypes type) {
    return new JsonSchema().addMember(name, new JsonSchema().addPrimitiveSchemaType(type));
  }
}
