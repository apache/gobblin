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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type;
import org.apache.gobblin.source.extractor.schema.Schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.ENUM;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.FIXED;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.RECORD;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.UNION;
import static org.apache.gobblin.converter.json.JsonSchema.SchemaType.CHILD;
import static org.apache.gobblin.converter.json.JsonSchema.SchemaType.ROOT;


/**
 * Represents a source schema declared in the configuration with {@link ConfigurationKeys#SOURCE_SCHEMA}.
 * The source schema is represented by a {@link JsonArray}.
 * @author tilakpatidar
 */
public class JsonSchema extends Schema {
  public static final String RECORD_FIELDS_KEY = "values";
  public static final String TYPE_KEY = "type";
  public static final String NAME_KEY = "name";
  public static final String SIZE_KEY = "size";
  public static final String ENUM_SYMBOLS_KEY = "symbols";
  public static final String COLUMN_NAME_KEY = "columnName";
  public static final String DATA_TYPE_KEY = "dataType";
  public static final String COMMENT_KEY = "comment";
  public static final String DEFAULT_VALUE_KEY = "defaultValue";
  public static final String IS_NULLABLE_KEY = "isNullable";
  public static final String DEFAULT_RECORD_COLUMN_NAME = "root";
  public static final String DEFAULT_VALUE_FOR_OPTIONAL_PROPERTY = "";
  public static final String ARRAY_ITEMS_KEY = "items";
  public static final String MAP_ITEMS_KEY = "values";
  public static final String SOURCE_TYPE = "source.type";
  private final Type type;
  private final JsonObject json;
  private final SchemaType schemaNestedLevel;
  private JsonSchema secondType;
  private JsonSchema firstType;
  private JsonArray jsonArray;

  public enum SchemaType {
    ROOT, CHILD
  }

  /**
   * Build a {@link JsonSchema} using {@link JsonArray}
   * This will create a {@link SchemaType} of {@link SchemaType#ROOT}
   * @param jsonArray
   */
  public JsonSchema(JsonArray jsonArray) {
    JsonObject jsonObject = new JsonObject();
    JsonObject dataType = new JsonObject();
    jsonObject.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
    dataType.addProperty(TYPE_KEY, RECORD.toString());
    dataType.add(RECORD_FIELDS_KEY, jsonArray);
    jsonObject.add(DATA_TYPE_KEY, dataType);
    setJsonSchemaProperties(jsonObject);
    this.type = RECORD;
    this.json = jsonObject;
    this.jsonArray = jsonArray;
    this.schemaNestedLevel = ROOT;
  }

  /**
   * Build a {@link JsonSchema} using {@link JsonArray}
   * This will create a {@link SchemaType} of {@link SchemaType#CHILD}
   * @param jsonObject
   */
  public JsonSchema(JsonObject jsonObject) {
    JsonObject root = new JsonObject();
    if (!jsonObject.has(COLUMN_NAME_KEY) && !jsonObject.has(DATA_TYPE_KEY)) {
      root.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
      root.add(DATA_TYPE_KEY, jsonObject);
      jsonObject = root;
    }
    if (!jsonObject.has(COLUMN_NAME_KEY) && jsonObject.has(DATA_TYPE_KEY)) {
      jsonObject.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
    }
    setJsonSchemaProperties(jsonObject);
    JsonElement typeElement = getDataType().get(TYPE_KEY);
    if (typeElement.isJsonPrimitive()) {
      this.type = Type.valueOf(typeElement.getAsString().toUpperCase());
    } else if (typeElement.isJsonArray()) {
      JsonArray jsonArray = typeElement.getAsJsonArray();
      if (jsonArray.size() != 2) {
        throw new RuntimeException("Invalid " + TYPE_KEY + "property in schema for union types");
      }
      this.type = UNION;
      JsonElement type1 = jsonArray.get(0);
      JsonElement type2 = jsonArray.get(1);
      if (type1.isJsonPrimitive()) {
        this.firstType = buildBaseSchema(Type.valueOf(type1.getAsString().toUpperCase()));
      }
      if (type2.isJsonPrimitive()) {
        this.secondType = buildBaseSchema(Type.valueOf(type2.getAsString().toUpperCase()));
      }
      if (type1.isJsonObject()) {
        this.firstType = buildBaseSchema(type1.getAsJsonObject());
      }
      if (type2.isJsonObject()) {
        this.secondType = buildBaseSchema(type2.getAsJsonObject());
      }
    } else {
      throw new RuntimeException("Invalid " + TYPE_KEY + "property in schema");
    }
    this.json = jsonObject;
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(jsonObject);
    this.jsonArray = jsonArray;
    this.schemaNestedLevel = CHILD;
  }

  /**
   * Get symbols for a {@link Type#ENUM} type.
   * @return
   */
  public JsonArray getSymbols() {
    if (this.type.equals(ENUM)) {
      return getDataType().get(ENUM_SYMBOLS_KEY).getAsJsonArray();
    }
    return new JsonArray();
  }

  /**
   * Get {@link Type} for this {@link JsonSchema}.
   * @return
   */
  public Type getType() {
    return type;
  }

  /**
   * Builds a {@link JsonSchema} object for a given {@link Type} object.
   * @param type
   * @return
   */
  public static JsonSchema buildBaseSchema(Type type) {
    JsonObject jsonObject = new JsonObject();
    JsonObject dataType = new JsonObject();
    jsonObject.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
    dataType.addProperty(TYPE_KEY, type.toString());
    jsonObject.add(DATA_TYPE_KEY, dataType);
    return new JsonSchema(jsonObject);
  }

  /**
   * Builds a {@link JsonSchema} object for a given {@link Type} object.
   * @return
   */
  public static JsonSchema buildBaseSchema(JsonObject root) {
    root.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
    return new JsonSchema(root);
  }

  /**
   * Get optional property from a {@link JsonObject} for a {@link String} key.
   * If key does'nt exists returns {@link #DEFAULT_VALUE_FOR_OPTIONAL_PROPERTY}.
   * @param jsonObject
   * @param key
   * @return
   */
  public static String getOptionalProperty(JsonObject jsonObject, String key) {
    return jsonObject.has(key) ? jsonObject.get(key).getAsString() : DEFAULT_VALUE_FOR_OPTIONAL_PROPERTY;
  }

  /**
   * Fetches dataType.values from the JsonObject
   * @return
   */
  public JsonSchema getValuesWithinDataType() {
    JsonElement element = this.getDataType().get(MAP_ITEMS_KEY);
    if (element.isJsonObject()) {
      return new JsonSchema(element.getAsJsonObject());
    }
    if (element.isJsonArray()) {
      return new JsonSchema(element.getAsJsonArray());
    }
    if (element.isJsonPrimitive()) {
      return buildBaseSchema(Type.valueOf(element.getAsString().toUpperCase()));
    }
    throw new UnsupportedOperationException(
        "Map values can only be defined using JsonObject, JsonArray or JsonPrimitive.");
  }

  /**
   * Gets size for fixed type viz dataType.size from the JsonObject
   * @return
   */
  public int getSizeOfFixedData() {
    if (this.type.equals(FIXED)) {
      return this.getDataType().get(SIZE_KEY).getAsInt();
    }
    return 0;
  }

  public boolean isType(Type type) {
    return this.type.equals(type);
  }

  /**
   * Fetches the nested or primitive array items type from schema.
   * @return
   * @throws DataConversionException
   */
  public Type getTypeOfArrayItems()
      throws DataConversionException {
    JsonSchema arrayValues = getItemsWithinDataType();
    if (arrayValues == null) {
      throw new DataConversionException("Array types only allow values as primitive, null or JsonObject");
    }
    return arrayValues.getType();
  }

  public JsonSchema getItemsWithinDataType() {
    JsonElement element = this.getDataType().get(ARRAY_ITEMS_KEY);
    if (element.isJsonObject()) {
      return new JsonSchema(element.getAsJsonObject());
    }
    if (element.isJsonPrimitive()) {
      return buildBaseSchema(Type.valueOf(element.getAsString().toUpperCase()));
    }
    throw new UnsupportedOperationException("Array items can only be defined using JsonObject or JsonPrimitive.");
  }

  public JsonSchema getFirstTypeSchema() {
    return this.firstType;
  }

  public JsonSchema getSecondTypeSchema() {
    return this.secondType;
  }

  public int fieldsCount() {
    return this.jsonArray.size();
  }

  public JsonSchema getFieldSchemaAt(int i) {
    if (i >= this.jsonArray.size()) {
      return new JsonSchema(this.json);
    }
    return new JsonSchema(this.jsonArray.get(i).getAsJsonObject());
  }

  public List<JsonSchema> getDataTypes() {
    if (firstType != null && secondType != null) {
      return Arrays.asList(firstType, secondType);
    }
    return Collections.singletonList(this);
  }

  public boolean isRoot() {
    return this.schemaNestedLevel.equals(ROOT);
  }

  public String getName() {
    return getOptionalProperty(this.getDataType(), NAME_KEY);
  }

  /**
   * Set properties for {@link JsonSchema} from a {@link JsonObject}.
   * @param jsonObject
   */
  private void setJsonSchemaProperties(JsonObject jsonObject) {
    setColumnName(jsonObject.get(COLUMN_NAME_KEY).getAsString());
    setDataType(jsonObject.get(DATA_TYPE_KEY).getAsJsonObject());
    setNullable(jsonObject.has(IS_NULLABLE_KEY) && jsonObject.get(IS_NULLABLE_KEY).getAsBoolean());
    setComment(getOptionalProperty(jsonObject, COMMENT_KEY));
    setDefaultValue(getOptionalProperty(jsonObject, DEFAULT_VALUE_KEY));
  }
}