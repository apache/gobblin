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
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;


/**
 * Recursively defined a Json Intermediate schema
 *
 * JsonIntermediateSchema := Map<columnName, JisColumn>
 *
 * JisColumn :=  (columnName, nullability, JisDataType)
 *
 * JisDataType := RecordType | ArrayType | EnumType | UnionType
 *
 * RecordType := (JsonElementType, JsonIntermediateSchema)
 *
 * ArrayType := (JsonElementType, JisDataType)
 *
 * EnumType := (JsonElementType, symbolsArray)
 *
 * UnionType := (JsonElementType, List<JisDataType>)
 *
 */


public class JsonIntermediateSchema {
  public static final String ROOT_RECORD_COLUMN_NAME = "root";
  public static final String CHILD_RECORD_COLUMN_NAME = "child";

  final private static String KEY_WORD_COLUMN_NAME = "columnName";
  final private static String KEY_WORD_DATA_TYPE = "dataType";
  final private static String KEY_WORD_DATA_IS_NULLABLE = "isNullable";
  final private static String KEY_WORD_ITEMS = "items";
  final private static String KEY_WORD_NAME = "name";
  final private static String KEY_WORD_SYMBOLS = "symbols";
  final private static String KEY_WORD_TYPE = "type";
  final private static String KEY_WORD_UNKNOWN = "unknown";
  final private static String KEY_WORD_VALUES = "values";

  @Getter
  Map<String, JisColumn> columns = new HashMap<>();

  @Getter
  @Setter
  String schemaName;

  // a JIS schema contains JIS columns
  public class JisColumn {
    @NonNull
    @Getter
    @Setter
    String columnName;

    @Getter
    @Setter
    Boolean isNullable;

    @Getter
    @Setter
    JisDataType dataType;

    // define a simple column
    JisColumn(String name, Boolean isNullable, String type) {
      this.setColumnName(name);
      this.setIsNullable(isNullable);
      this.setDataType(new JisDataType(type));
    }

    // define a complex column
    JisColumn(JsonObject columnDefinition) {
      try {
        if (columnDefinition.has(KEY_WORD_COLUMN_NAME)) {
          this.setColumnName(columnDefinition.get(KEY_WORD_COLUMN_NAME).getAsString());
        } else if (columnDefinition.has(KEY_WORD_NAME)) {
          this.setColumnName(columnDefinition.get(KEY_WORD_COLUMN_NAME).getAsString());
        } else {
          this.setColumnName(KEY_WORD_UNKNOWN);
        }

        // set default as NULLABLE if column definition did not specify
        if (columnDefinition.has(KEY_WORD_DATA_IS_NULLABLE)) {
          this.setIsNullable(Boolean.valueOf(columnDefinition.get(KEY_WORD_DATA_IS_NULLABLE).getAsString()));
        } else {
          this.setIsNullable(Boolean.TRUE);
        }

        this.setDataType(new JisDataType(columnDefinition.get(KEY_WORD_DATA_TYPE).getAsJsonObject()));
      } catch (Exception e) {
        throw new RuntimeException("Incorrect column definition in JSON: " + columnDefinition.toString());
      }
    }

    /**
     * Convert the column object to Json Object
     * @return a Json Object presentation of the column
     */
    public JsonObject toJson() {
      JsonObject column = new JsonObject();
      column.addProperty(KEY_WORD_COLUMN_NAME, this.getColumnName());
      column.addProperty(KEY_WORD_DATA_IS_NULLABLE, this.isNullable ? "true" : "false");
      column.add(KEY_WORD_DATA_TYPE, this.getDataType().toJson());
      return column;
    }
  }

  // a JIS Column has a JIS Data Type
  public class JisDataType {
    @NonNull
    @Getter
    @Setter
    JsonElementTypes type;

    // data type name is optional
    @Getter
    @Setter
    String name;

    // values have the array of field definitions when the type is record
    @Getter
    @Setter
    JsonIntermediateSchema childRecord;

    // items have the item definition
    @Getter
    @Setter
    JisDataType itemType;

    // unions have item types
    @Getter
    @Setter
    List<JisDataType> itemTypes = Lists.newArrayList();

    @Getter
    @Setter
    JsonArray symbols;

    // this defines primitive data type
    JisDataType(String type) {
      this.setType(JsonElementTypes.forType(type));
    }

    JisDataType(JsonObject dataTypeDefinition) {
      this.setType(JsonElementTypes.forType(dataTypeDefinition.get(KEY_WORD_TYPE).getAsString()));
      if (dataTypeDefinition.has(KEY_WORD_NAME)) {
        this.setName(dataTypeDefinition.get(KEY_WORD_NAME).getAsString());
      }
      switch (type) {
        case RECORD:
          // a record field is will have child schema
          this.setChildRecord(new JsonIntermediateSchema(CHILD_RECORD_COLUMN_NAME,
              dataTypeDefinition.get(KEY_WORD_VALUES).getAsJsonArray()));
          break;
        case ARRAY:
          // an array field will have a item type definition, which can be primitive or complex
          JsonElement itemDefinition = dataTypeDefinition.get(KEY_WORD_ITEMS);

          if (itemDefinition.isJsonPrimitive()) {
            this.setItemType(new JisDataType(itemDefinition.getAsString()));
          } else {
            // if not primitive, the item type is complex, and it has to be defined in a JsonObject
            this.setItemType(new JisDataType(itemDefinition.getAsJsonObject().get(KEY_WORD_DATA_TYPE).getAsJsonObject()));
          }
          break;
        case ENUM:
          // an Enum has a list of symbols
          this.setSymbols(dataTypeDefinition.get(KEY_WORD_SYMBOLS).getAsJsonArray());
          break;
        case UNION:
          // a Union has 2 or more different types
          // TODO
          break;
        default:
          break;
      }
    }

    /** Convert the data type object to Json Object
     * @return a Json Object presentation of the data type
     */
    public JsonObject toJson() {
      JsonObject dataType = new JsonObject();
      dataType.addProperty(KEY_WORD_TYPE, this.getType().toString());
      dataType.addProperty(KEY_WORD_NAME, this.getName());
      switch (type) {
        case RECORD:
          dataType.add(KEY_WORD_VALUES, childRecord.toJson());
          break;
        case ARRAY:
          JsonObject itemsObject = new JsonObject();
          itemsObject.addProperty(KEY_WORD_NAME, this.getName());
          itemsObject.add(KEY_WORD_DATA_TYPE, itemType.toJson());
          dataType.add(KEY_WORD_ITEMS, itemsObject);
          break;
        default:
          break;
      }
      return dataType;
    }

    public boolean isPrimitive() {
      return JsonElementTypes.isPrimitive(type);
    }
  }

  /**
   * A Json Intermediate schema starts with a root column
   *
   * @param recordSchema the intermediate schema definition
   */
  public JsonIntermediateSchema(JsonArray recordSchema) {
    this.setSchemaName(ROOT_RECORD_COLUMN_NAME);
    addColumns(recordSchema);
  }

  /**
   * A Json Intermediate schema record can be a nested field
   *
   * @param fieldName the field name of the nested record
   * @param recordSchema the intermediate schema definition
   */
  public JsonIntermediateSchema(String fieldName, JsonArray recordSchema) {
    this.setSchemaName(fieldName);
    addColumns(recordSchema);
  }

  /**
   * add columns of a record
   * @param recordSchema the schema of the record
   */
  private void addColumns(JsonArray recordSchema) {
    for (JsonElement column: recordSchema) {
      Preconditions.checkArgument(column != null && column.isJsonObject());
      JisColumn col = new JisColumn(column.getAsJsonObject());
      columns.put(col.getColumnName(), col);
    }
  }

  /**
   * Convert the schema object to Json Array
   * @return a Json Array presentation of the schema
   */
  public JsonArray toJson() {
    JsonArray schema = new JsonArray();
    for (Map.Entry<String, JisColumn> entry: columns.entrySet()) {
      schema.add(entry.getValue().toJson());
    }
    return schema;
  }
}
