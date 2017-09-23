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
import java.util.List;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.source.extractor.schema.Schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import static org.apache.gobblin.converter.json.JsonSchema.InputType.*;
import static org.apache.gobblin.converter.json.JsonStringToJsonIntermediateConverter.JsonUtils.jsonArray;


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
  public static final String DEFAULT_RECORD_COLUMN_NAME = "temp";
  public static final String DEFAULT_VALUE_FOR_OPTIONAL_PROPERTY = "";
  public static final String RECORD_ITEMS_KEY = "values";
  public static final String ARRAY_KEY = "item";
  public static final String ARRAY_ITEMS_KEY = "items";
  public static final String MAP_ITEMS_KEY = "values";
  public static final String MAP_KEY = "map";
  public static final String MAP_KEY_COLUMN_NAME = "key";
  public static final String MAP_VALUE_COLUMN_NAME = "value";
  private final InputType type;
  private final JsonObject json;
  private JsonSchema secondType;
  private JsonSchema firstType;
  private int size;
  private JsonArray jsonArray;

  public enum InputType {
    DATE,
    TIMESTAMP,
    TIME,
    FIXED,
    STRING,
    BYTES,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    ARRAY,
    MAP,
    ENUM,
    RECORD,
    NULL,
    UNION;

    static List<InputType> primitiveTypes =
        Arrays.asList(NULL, BOOLEAN, INT, LONG, FLOAT, DOUBLE, BYTES, STRING, ENUM, FIXED);
  }

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
    this.size = jsonArray.size();
    this.jsonArray = jsonArray;
  }

  public JsonSchema(JsonObject jsonObject) {
    setJsonSchemaProperties(jsonObject);
    JsonElement typeElement = getDataType().get(TYPE_KEY);
    if (typeElement.isJsonPrimitive()) {
      this.type = InputType.valueOf(typeElement.getAsString().toUpperCase());
    } else if (typeElement.isJsonArray()) {
      JsonArray jsonArray = typeElement.getAsJsonArray();
      if (jsonArray.size() != 2) {
        throw new RuntimeException("Invalid " + TYPE_KEY + "property in schema");
      }
      this.type = UNION;
      JsonElement type1 = jsonArray.get(0);
      JsonElement type2 = jsonArray.get(1);
      if (type1.isJsonPrimitive()) {
        this.firstType = buildBaseSchema(InputType.valueOf(type1.getAsString().toUpperCase()));
      }
      if (type2.isJsonPrimitive()) {
        this.secondType = buildBaseSchema(InputType.valueOf(type2.getAsString().toUpperCase()));
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
    this.size = 1;
  }

  /**
   * Get source.schema within a {@link InputType#RECORD} type.
   * The source.schema is represented by a {@link JsonArray}
   * @return
   */
  public JsonArray getDataTypeValues() {
    if (this.type.equals(RECORD)) {
      return getDataType().get(RECORD_FIELDS_KEY).getAsJsonArray();
    }
    return new JsonArray();
  }

  /**
   * Get symbols for a {@link InputType#ENUM} type.
   * @return
   */
  public JsonArray getSymbols() {
    if (this.type.equals(ENUM)) {
      return getDataType().get(ENUM_SYMBOLS_KEY).getAsJsonArray();
    }
    return new JsonArray();
  }

  /**
   * Get {@link InputType} for this {@link JsonSchema}.
   * @return
   */
  public InputType getInputType() {
    return type;
  }

  /**
   * Builds a {@link JsonSchema} object for a given {@link InputType} object.
   * @param type
   * @return
   */
  public static JsonSchema buildBaseSchema(InputType type) {
    JsonObject jsonObject = new JsonObject();
    JsonObject dataType = new JsonObject();
    jsonObject.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
    dataType.addProperty(TYPE_KEY, type.toString());
    jsonObject.add(DATA_TYPE_KEY, dataType);
    return new JsonSchema(jsonObject);
  }

  /**
   * Builds a {@link JsonSchema} object for a given {@link InputType} object.
   * @return
   */
  public static JsonSchema buildBaseSchema(JsonObject root) {
    root.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
    return new JsonSchema(root);
  }

  /**
   * {@link InputType} of the elements composed within complex type.
   * @param itemKey
   * @return
   */
  public InputType getElementTypeUsingKey(String itemKey) {
    String type = this.getDataType().get(itemKey).getAsString().toUpperCase();
    return InputType.valueOf(type);
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

  /**
   * Get optional property from a {@link JsonObject} for a {@link String} key.
   * If key does'nt exists returns {@link #DEFAULT_VALUE_FOR_OPTIONAL_PROPERTY}.
   * @param jsonObject
   * @param key
   * @return
   */
  private String getOptionalProperty(JsonObject jsonObject, String key) {
    return jsonObject.has(key) ? jsonObject.get(key).getAsString() : DEFAULT_VALUE_FOR_OPTIONAL_PROPERTY;
  }

  /**
   * Fetches dataType.values from the JsonObject
   * @return
   */
  public JsonElement getValuesWithinDataType() {
    return this.getDataType().get(RECORD_ITEMS_KEY);
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

  /**
   * Gets schema of record which is present within array schema.
   * @return
   */
  public JsonArray getSchemaForArrayHavingRecord() {
    JsonObject root = new JsonObject();
    JsonObject dataType = getItemsWithinDataType().getAsJsonObject();
    root.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
    root.add(DATA_TYPE_KEY, dataType);
    return new JsonSchema(root).getValuesWithinDataType().getAsJsonArray();
  }

  public boolean isEnumType() {
    return this.type.equals(ENUM);
  }

  /**
   * Get dataType.type from schema
   * @return
   */
  public JsonArray getDataTypeTypeFromSchema() {
    JsonObject dataType = this.getDataType();
    if (dataType.has(TYPE_KEY)) {
      if (dataType.get(TYPE_KEY).isJsonPrimitive()) {
        return jsonArray(dataType.get(TYPE_KEY));
      } else {
        if (dataType.get(TYPE_KEY).isJsonArray()) {
          return dataType.get(TYPE_KEY).getAsJsonArray();
        } else {
          return null;
        }
      }
    } else {
      return null;
    }
  }

  public boolean isMapType() {
    return this.type.equals(MAP);
  }

  public boolean isRecordType() {
    return this.type.equals(RECORD);
  }

  public boolean isUnionType() {
    return this.type.equals(UNION);
  }

  public boolean isPrimitiveType() {
    return InputType.primitiveTypes.contains(this.type);
  }

  public boolean isFixedType() {
    return this.type.equals(FIXED);
  }

  public boolean isArrayType() {
    return this.type.equals(ARRAY);
  }

  public boolean isTypeEqual(JsonSchema schema) {
    return this.type.equals(schema.getInputType());
  }

  public boolean isNullType() {
    return this.type.equals(NULL);
  }

  /**
   * Fetches the nested or primitive array items type from schema.
   * @return
   * @throws DataConversionException
   */
  public InputType arrayType()
      throws DataConversionException {
    if (!isArrayType()) {
      return null;
    }

    JsonElement arrayValues = getItemsWithinDataType();

    try {
      if (arrayValues.isJsonPrimitive() && arrayValues.getAsJsonPrimitive().isString()) {
        return InputType.valueOf(arrayValues.getAsString().toUpperCase());
      } else {
        JsonObject root = new JsonObject();
        root.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
        root.add(DATA_TYPE_KEY, arrayValues.getAsJsonObject());
        return InputType.valueOf(new JsonSchema(root).getDataTypeTypeFromSchema().get(0).getAsString());
      }
    } catch (UnsupportedOperationException | IllegalStateException e) {
      //values is not string and a nested json array
      throw new DataConversionException("Array types only allow values as primitive, null or JsonObject");
    }
  }

  public JsonElement getItemsWithinDataType() {
    return this.getDataType().get(ARRAY_ITEMS_KEY);
  }

  public String getPropOrBlankString(String key) {
    return this.json.has(key) ? this.json.get(key).getAsString() : "";
  }

  public boolean getAsBooleanIfExists(String key) {
    return this.json.has(key) && this.json.get(key).getAsBoolean();
  }

  public JsonSchema getFirstInputType() {
    if (!isUnionType()) {
      return null;
    }
    return this.firstType;
  }

  public JsonSchema getSecondInputType() {
    if (!isUnionType()) {
      return null;
    }
    return this.secondType;
  }

  public int size() {
    return this.size;
  }

  public JsonElement get() {
    return this.json;
  }

  public JsonSchema get(int i) {
    return new JsonSchema(this.jsonArray.get(i).getAsJsonObject());
  }
}