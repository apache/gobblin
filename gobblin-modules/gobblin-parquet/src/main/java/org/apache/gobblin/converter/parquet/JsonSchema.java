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
package org.apache.gobblin.converter.parquet;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.extractor.schema.Schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import parquet.schema.Type.Repetition;

import static org.apache.gobblin.converter.parquet.JsonSchema.InputType.ENUM;
import static org.apache.gobblin.converter.parquet.JsonSchema.InputType.RECORD;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REQUIRED;


/**
 * Represents a source schema declared in the configuration with {@link ConfigurationKeys#SOURCE_SCHEMA}.
 * The source schema is represented by a {@link JsonArray}.
 * @author tilakpatidar
 */
public class JsonSchema extends Schema {
  public static final String RECORD_FIELDS_KEY = "values";
  public static final String TYPE_KEY = "type";
  public static final String ENUM_SYMBOLS_KEY = "symbols";
  public static final String COLUMN_NAME_KEY = "columnName";
  public static final String DATA_TYPE_KEY = "dataType";
  public static final String COMMENT_KEY = "comment";
  public static final String DEFAULT_VALUE_KEY = "defaultValue";
  public static final String IS_NULLABLE_KEY = "isNullable";
  public static final String DEFAULT_RECORD_COLUMN_NAME = "temp";
  public static final String DEFAULT_VALUE_FOR_OPTIONAL_PROPERTY = "";
  public static final String ARRAY_KEY = "item";
  public static final String ARRAY_ITEMS_KEY = "items";
  public static final String MAP_ITEMS_KEY = "values";
  public static final String MAP_KEY = "map";
  public static final String MAP_KEY_COLUMN_NAME = "key";
  public static final String MAP_VALUE_COLUMN_NAME = "value";
  private final InputType type;

  public enum InputType {
    STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ARRAY, ENUM, RECORD, MAP
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
  }

  public JsonSchema(JsonObject jsonobject) {
    setJsonSchemaProperties(jsonobject);
    this.type = InputType.valueOf(getDataType().get(TYPE_KEY).getAsString().toUpperCase());
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
   * {@link InputType} of the elements composed within complex type.
   * @param itemKey
   * @return
   */
  public InputType getElementTypeUsingKey(String itemKey) {
    String type = this.getDataType().get(itemKey).getAsString().toUpperCase();
    return InputType.valueOf(type);
  }

  /**
   * Parquet {@link Repetition} for this {@link JsonSchema}.
   * @return
   */
  public Repetition optionalOrRequired() {
    return this.isNullable() ? OPTIONAL : REQUIRED;
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
}
