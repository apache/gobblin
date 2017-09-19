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

import org.apache.gobblin.source.extractor.schema.Schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import parquet.schema.Type;

import static org.apache.gobblin.converter.parquet.JsonSchema.DataType.ENUM;
import static org.apache.gobblin.converter.parquet.JsonSchema.DataType.RECORD;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REQUIRED;


/**
 * Builds a {@link Schema} from jsonSchema represented using json string or JsonArray.
 */
public class JsonSchema extends Schema {
  private static final String RECORD_FIELDS_KEY = "values";
  private static final String DEFAULT_RECORD_COLUMN_NAME = "temp";
  private static final String TYPE_KEY = "type";
  private static final String ENUM_SYMBOLS_KEY = "symbols";
  private static final String COLUMN_NAME_KEY = "columnName";
  private static final String DATA_TYPE_KEY = "dataType";
  private static final String COMMENT_KEY = "comment";
  private static final String DEFAULT_VALUE_KEY = "defaultValue";
  private static final String IS_NULLABLE_KEY = "isNullable";
  private final DataType type;

  public enum DataType {
    STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ARRAY, ENUM, RECORD, MAP
  }

  public JsonSchema(JsonArray jsonArray) {
    JsonObject temp = buildTempRecord(jsonArray);
    setSuper(temp);
    this.type = RECORD;
  }

  public JsonSchema(JsonObject jsonObject) {
    setSuper(jsonObject);
    this.type = DataType.valueOf(super.getDataType().get(TYPE_KEY).getAsString().toUpperCase());
  }

  /**
   * Gets fields within a record type
   * @return
   */
  public JsonArray getDataTypeValues() {
    if (this.type.equals(RECORD)) {
      return super.getDataType().get(RECORD_FIELDS_KEY).getAsJsonArray();
    }
    return new JsonArray();
  }

  /**
   * Gets symbols within a enum type
   * @return
   */
  public JsonArray getSymbols() {
    if (this.type.equals(ENUM)) {
      return this.getDataType().get(ENUM_SYMBOLS_KEY).getAsJsonArray();
    }
    return new JsonArray();
  }

  /**
   * Get source type
   * @return
   */
  public DataType getType() {
    return type;
  }

  /**
   * Builds a temp element schema based on type
   * @param type
   * @return
   */
  public static JsonSchema buildElementSchema(DataType type) {
    JsonObject temp = new JsonObject();
    JsonObject dataType = new JsonObject();
    temp.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
    dataType.addProperty(TYPE_KEY, type.toString());
    temp.add(DATA_TYPE_KEY, dataType);
    return new JsonSchema(temp);
  }

  /**
   * Parquet RepetitionType Optional or Required
   * @return
   */
  protected Type.Repetition optionalOrRequired() {
    return this.isNullable() ? OPTIONAL : REQUIRED;
  }

  private void setSuper(JsonObject jsonObject) {
    super.setColumnName(jsonObject.get(COLUMN_NAME_KEY).getAsString());
    super.setComment(jsonObject.has(COMMENT_KEY) ? jsonObject.get(COMMENT_KEY).getAsString() : "");
    super.setDataType(jsonObject.get(DATA_TYPE_KEY).getAsJsonObject());
    super.setDefaultValue(jsonObject.has(DEFAULT_VALUE_KEY) ? jsonObject.get(DEFAULT_VALUE_KEY).getAsString() : "");
    super.setNullable(jsonObject.has(IS_NULLABLE_KEY) && jsonObject.get(IS_NULLABLE_KEY).getAsBoolean());
  }

  private JsonObject buildTempRecord(JsonArray jsonArray) {
    JsonObject temp = new JsonObject();
    JsonObject dataType = new JsonObject();
    temp.addProperty(COLUMN_NAME_KEY, DEFAULT_RECORD_COLUMN_NAME);
    dataType.addProperty(TYPE_KEY, RECORD.toString());
    dataType.add(RECORD_FIELDS_KEY, jsonArray);
    temp.add(DATA_TYPE_KEY, dataType);
    return temp;
  }
}
