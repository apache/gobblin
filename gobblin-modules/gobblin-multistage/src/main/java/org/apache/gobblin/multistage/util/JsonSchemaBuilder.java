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
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;


/**
 * This utility class aims to simplify the structure manipulation of JsonSchema.
 *
 * At the same time, we are deprecating JsonSchema class
 */
public class JsonSchemaBuilder {
  final private static String KEY_WORD_ARRAY = "array";
  final private static String KEY_WORD_COLUMN_NAME = "columnName";
  final private static String KEY_WORD_DATA_TYPE = "dataType";
  final private static String KEY_WORD_FIELDS = "fields";
  final private static String KEY_WORD_IS_NULLABLE = "isNullable";
  final private static String KEY_WORD_ITEMS = "items";
  final private static String KEY_WORD_NAME = "name";
  final private static String KEY_WORD_NULLABLE = "nullable";
  final private static String KEY_WORD_RECORD = "record";
  final private static String KEY_WORD_SOURCE_TYPE = "source.type";
  final private static String KEY_WORD_TYPE = "type";
  final private static String KEY_WORD_UNKNOWN = "unknown";
  final private static String KEY_WORD_VALUES = "values";
  final private static JsonElement JSON_NULL_STRING = new JsonPrimitive("null");

  final private static int UNKNOWN = 0;
  final private static int RECORD = 1;
  final private static int ARRAY = 2;
  final private static int PRIMITIVE = 3;

  final private static ImmutableMap<String, Integer> COMMON = ImmutableMap.of(
      KEY_WORD_RECORD, RECORD,
      KEY_WORD_ARRAY, ARRAY);

  private int type = UNKNOWN;
  @Getter
  private String name;
  private String primitiveType = null;
  private boolean isNullable = true;
  List<JsonSchemaBuilder> elements = new ArrayList<>();

  /**
   * This is main method to parse an Avro schema, typically read from TMS,
   * and make it into a builder.
   *
   * @param json the Json object that was parsed from an Avro schema string
   * @return a builder that can then be called to buildAltSchema for converter
   */
  static JsonSchemaBuilder fromAvro(JsonElement json) {
    return new JsonSchemaBuilder(RECORD, json.getAsJsonObject().get(KEY_WORD_FIELDS));
  }

  /**
   * Hidden constructor that is called by public static function only
   * @param type the type of the schema element
   * @param json the Json presentation of the schema element
   */
  private JsonSchemaBuilder(final int type, JsonElement json) {
    this("root", type, json);
  }

  /**
   * Hidden constructor that is called by internal parsing functions only
   * @param name the name of the schema element
   * @param type the type of the schema element
   * @param json the Json presentation of the schema element
   */
  private JsonSchemaBuilder(final String name, final int type, final JsonElement json) {
    initialize(name, type, json);
  }

  /**
   * Hidden initialization function
   * @param name the name of the schema element
   * @param type the type of the schema element
   * @param json the Json presentation of the schema element
   */
  private void initialize(final String name, final int type, final JsonElement json) {
    this.type = type == UNKNOWN ? getType(json) : type;
    this.isNullable = !name.equals("root") && checkNullable(json.getAsJsonObject());
    this.name = name;
    switch (this.type) {
      case RECORD:
        if (name.equals("root")) {
          for (JsonElement field: json.getAsJsonArray()) {
            elements.add(new JsonSchemaBuilder(field.getAsJsonObject().get(KEY_WORD_NAME).getAsString(), UNKNOWN, field));
          }
        } else {
          for (JsonElement field: getFields(json.getAsJsonObject())) {
            elements.add(new JsonSchemaBuilder(field.getAsJsonObject().get(KEY_WORD_NAME).getAsString(), UNKNOWN, field));
          }
        }
        break;
      case ARRAY:
        elements.add(new JsonSchemaBuilder("arrayItem", UNKNOWN, getItems(json.getAsJsonObject())));
        break;
      default: //PRIMITIVE
        this.primitiveType = getNestedType(json.getAsJsonObject());
        break;
    }
  }

  /**
   * Get the schema element type
   * @param json the Json presentation of the schema element
   * @return the schema element type
   */
  private int getType(JsonElement json) {
    if (json.isJsonPrimitive()) {
      return getType(json.getAsString());
    }
    return getType(getNestedType(json.getAsJsonObject()));
  }

  /**
   * Get the schema element type string from a straight or a unionized schema element
   * @param json the Json presentation of the schema element
   * @return the schema element type string
   */
  private String getNestedType(JsonObject json) {
    JsonElement type = json.get(KEY_WORD_TYPE);
    if (type.isJsonPrimitive()) {
      return type.getAsString();
    }
    if (type.isJsonObject()) {
      if (type.getAsJsonObject().has(KEY_WORD_SOURCE_TYPE)) {
        return type.getAsJsonObject().get(KEY_WORD_SOURCE_TYPE).getAsString();
      }
      return type.getAsJsonObject().get(KEY_WORD_TYPE).getAsString();
    }
    if (type.isJsonArray()) {
      Set<JsonElement> items = new HashSet<>();
      type.getAsJsonArray().iterator().forEachRemaining(items::add);
      items.remove(JSON_NULL_STRING);
      JsonObject trueType = items.iterator().next().getAsJsonObject();
      if (trueType.has(KEY_WORD_SOURCE_TYPE)) {
        return trueType.get(KEY_WORD_SOURCE_TYPE).getAsString();
      }
      return trueType.get(KEY_WORD_TYPE).getAsString();
    }
    return KEY_WORD_UNKNOWN;
  }

  /**
   * Map a string type to internal presentation of integer type
   * @param type the schema element type string
   * @return the schema element type integer
   */
  private int getType(String type) {
    return COMMON.getOrDefault(type, PRIMITIVE);
  }

  /**
   * Check if an schema element is nullable, this is for AVRO schema parsing only
   * @param json the Json presentation of the schema element
   * @return nullability
   */
  private boolean checkNullable(JsonObject json) {
    JsonElement type = json.get(KEY_WORD_TYPE);
    if (type.isJsonPrimitive()) {
      return type.equals(JSON_NULL_STRING);
    }

    if (type.isJsonObject()) {
      return type.getAsJsonObject().get(KEY_WORD_TYPE).equals(JSON_NULL_STRING);
    }

    if (type.isJsonArray()) {
      return type.getAsJsonArray().contains(JSON_NULL_STRING);
    }

    return true;
  }

  /**
   * Parsing out the "fields" from an Avro schema
   * @param record the "record" schema element
   * @return the fields of the record
   */
  private JsonArray getFields(JsonObject record) {
    if (record.has(KEY_WORD_FIELDS)) {
      return record.get(KEY_WORD_FIELDS).getAsJsonArray();
    }

    return record.get(KEY_WORD_TYPE).getAsJsonObject().get(KEY_WORD_FIELDS).getAsJsonArray();
  }

  /**
   * Parsing out the array items in an Avro schema, current an array item can be a record,
   * a primitive, a null, or a union of any two of them. However, the union shall not be
   * more than 2 types.
   *
   * @param array the "array" schema element
   * @return the array item
   */
  private JsonObject getItems(JsonObject array) {
    if (array.get(KEY_WORD_TYPE).isJsonObject()) {
      return array.get(KEY_WORD_TYPE).getAsJsonObject().get(KEY_WORD_ITEMS).getAsJsonObject();
    }

    Set<JsonElement> union = new HashSet<>();
    array.get(KEY_WORD_TYPE).getAsJsonArray().iterator().forEachRemaining(union::add);
    union.remove(JSON_NULL_STRING);
    return union.iterator().next().getAsJsonObject().get(KEY_WORD_ITEMS).getAsJsonObject();
  }

  /**
   * Build into a JsonSchema for backward compatibility
   * @return JsonSchema object
   */
  public JsonSchema buildJsonSchema() {
    switch (this.type) {
      case RECORD:
        Preconditions.checkArgument(!isNullable);
        JsonSchema fields = new JsonSchema();
        for (JsonSchemaBuilder field: elements) {
          fields.addMember(field.getName(), field.buildJsonSchema());
        }
        if (name.equals("root")) {
          return new JsonSchema().addChildAsArray(fields);
        } else if (name.equals("arrayItem")) {
          return new JsonSchema().addChildAsObject(fields);
        } else {
          return new JsonSchema().addMember(name, new JsonSchema().addChildAsObject(fields));
        }
      case ARRAY:
        return new JsonSchema().addMember(name,
            new JsonSchema().addChildAsArray(elements.get(0).buildJsonSchema()));
      default: //PRIMITIVE
        String nullableType = isNullable ? KEY_WORD_NULLABLE + primitiveType : primitiveType;
        return new JsonSchema().addPrimitiveSchemaType(JsonElementTypes.valueOf(nullableType.toUpperCase()));
    }
  }

  /**
   * Build into a Avro flavored, but not true Avro, schema that can be fed into
   * Json2Avro converter
   *
   * @return Json presentation of the Avro flavored schema
   */
  public JsonElement buildAltSchema() {
    JsonObject nestedType = new JsonObject();
    if (this.type == RECORD) {
      Preconditions.checkArgument(!isNullable);
      JsonArray fields = new JsonArray();
      for (JsonSchemaBuilder field : elements) {
        fields.add(field.buildAltSchema());
      }
      if (name.equals("root")) {
        return fields;
      }
      nestedType.addProperty(KEY_WORD_TYPE, KEY_WORD_RECORD);
      nestedType.addProperty(KEY_WORD_NAME, this.name);
      nestedType.add(KEY_WORD_VALUES, fields);
    } else if (this.type == ARRAY) {
      nestedType.addProperty(KEY_WORD_TYPE, KEY_WORD_ARRAY);
      nestedType.addProperty(KEY_WORD_NAME, this.name);
      nestedType.add(KEY_WORD_ITEMS, this.elements.get(0).buildAltSchema());
    } else {
        nestedType.addProperty(KEY_WORD_TYPE, this.primitiveType);
    }

    JsonObject column = new JsonObject();
    column.addProperty(KEY_WORD_COLUMN_NAME, this.name);
    column.addProperty(KEY_WORD_IS_NULLABLE, this.isNullable);
    column.add(KEY_WORD_DATA_TYPE, nestedType);
    return column;
  }
}
