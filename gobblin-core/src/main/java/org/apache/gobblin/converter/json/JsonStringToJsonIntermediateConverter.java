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

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.FIXED;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.MAP;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.NULL;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.RECORD;
import static org.apache.gobblin.converter.json.JsonSchema.DEFAULT_RECORD_COLUMN_NAME;


/**
 * Converts a json string to a {@link JsonObject}.
 */
public class JsonStringToJsonIntermediateConverter extends Converter<String, JsonArray, String, JsonObject> {

  private final static Logger log = LoggerFactory.getLogger(JsonStringToJsonIntermediateConverter.class);

  private static final String UNPACK_COMPLEX_SCHEMAS_KEY =
      "gobblin.converter.jsonStringToJsonIntermediate.unpackComplexSchemas";
  public static final boolean DEFAULT_UNPACK_COMPLEX_SCHEMAS_KEY = Boolean.TRUE;

  private boolean unpackComplexSchemas;

  /**
   * Take in an input schema of type string, the schema must be in JSON format
   * @return a JsonArray representation of the schema
   */
  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    this.unpackComplexSchemas =
        workUnit.getPropAsBoolean(UNPACK_COMPLEX_SCHEMAS_KEY, DEFAULT_UNPACK_COMPLEX_SCHEMAS_KEY);

    JsonParser jsonParser = new JsonParser();
    log.info("Schema: " + inputSchema);
    JsonElement jsonSchema = jsonParser.parse(inputSchema);
    return jsonSchema.getAsJsonArray();
  }

  /**
   * Takes in a record with format String and Uses the inputSchema to convert the record to a JsonObject
   * @return a JsonObject representing the record
   * @throws IOException
   */
  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, String strInputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    JsonParser jsonParser = new JsonParser();
    JsonObject inputRecord = (JsonObject) jsonParser.parse(strInputRecord);

    if (!this.unpackComplexSchemas) {
      return new SingleRecordIterable<>(inputRecord);
    }
    JsonSchema schema = new JsonSchema(outputSchema);
    JsonObject rec = parse(inputRecord, schema);
    return new SingleRecordIterable(rec);
  }

  /**
   * Parses a provided JsonObject input using the provided JsonArray schema into
   * a JsonObject.
   * @param element
   * @param schema
   * @return
   * @throws DataConversionException
   */
  private JsonElement parse(JsonElement element, JsonSchema schema)
      throws DataConversionException {
    JsonObject root = new JsonObject();
    root.add(DEFAULT_RECORD_COLUMN_NAME, element);
    JsonObject jsonObject = parse(root, schema);
    return jsonObject.get(DEFAULT_RECORD_COLUMN_NAME);
  }

  /**
   * Parses a provided JsonObject input using the provided JsonArray schema into
   * a JsonObject.
   * @param record
   * @param schema
   * @return
   * @throws DataConversionException
   */
  private JsonObject parse(JsonObject record, JsonSchema schema)
      throws DataConversionException {
      JsonObject output = new JsonObject();
      for (int i = 0; i < schema.fieldsCount(); i++) {
        JsonSchema schemaElement = schema.getFieldSchemaAt(i);
        String columnKey = schemaElement.getColumnName();
        JsonElement parsed;
        if (!record.has(columnKey)) {
          output.add(columnKey, JsonNull.INSTANCE);
          continue;
        }

        JsonElement columnValue = record.get(columnKey);
        switch (schemaElement.getType()) {
          case UNION:
            parsed = parseUnionType(schemaElement, columnValue);
            break;
          case ENUM:
            parsed = parseEnumType(schemaElement, columnValue);
            break;
          default:
            if (columnValue.isJsonArray()) {
              parsed = parseJsonArrayType(schemaElement, columnValue);
            } else if (columnValue.isJsonObject()) {
              parsed = parseJsonObjectType(schemaElement, columnValue);
            } else {
              parsed = parsePrimitiveType(schemaElement, columnValue);
            }
        }
        output.add(columnKey, parsed);
      }
      return output;
  }

  private JsonElement parseUnionType(JsonSchema schemaElement, JsonElement columnValue)
      throws DataConversionException {
    try {
      return parse(columnValue, schemaElement.getFirstTypeSchema());
    } catch (DataConversionException e) {
      return parse(columnValue, schemaElement.getSecondTypeSchema());
    }
  }

  /**
   * Parses Enum type values
   * @param schema
   * @param value
   * @return
   * @throws DataConversionException
   */
  private JsonElement parseEnumType(JsonSchema schema, JsonElement value)
      throws DataConversionException {
    if (schema.getSymbols().contains(value)) {
      return value;
    }
    throw new DataConversionException(
        "Invalid symbol: " + value.getAsString() + " allowed values: " + schema.getSymbols().toString());
  }

  /**
   * Parses JsonArray type values
   * @param schema
   * @param value
   * @return
   * @throws DataConversionException
   */
  private JsonElement parseJsonArrayType(JsonSchema schema, JsonElement value)
      throws DataConversionException {
    Type arrayType = schema.getTypeOfArrayItems();
    JsonArray tempArray = new JsonArray();
    if (Type.isPrimitive(arrayType)) {
      return value;
    }
    JsonSchema nestedSchema = schema.getItemsWithinDataType();
    for (JsonElement v : value.getAsJsonArray()) {
      tempArray.add(parse(v, nestedSchema));
    }
    return tempArray;
  }

  /**
   * Parses JsonObject type values
   * @param value
   * @return
   * @throws DataConversionException
   */
  private JsonElement parseJsonObjectType(JsonSchema schema, JsonElement value)
      throws DataConversionException {
    JsonSchema valuesWithinDataType = schema.getValuesWithinDataType();
    if (schema.isType(MAP)) {
      if (Type.isPrimitive(valuesWithinDataType.getType())) {
        return value;
      }

      JsonObject map = new JsonObject();
      for (Entry<String, JsonElement> mapEntry : value.getAsJsonObject().entrySet()) {
        JsonElement mapValue = mapEntry.getValue();
        map.add(mapEntry.getKey(), parse(mapValue, valuesWithinDataType));
      }
      return map;
    } else if (schema.isType(RECORD)) {
      JsonSchema schemaArray = valuesWithinDataType.getValuesWithinDataType();
      return parse((JsonObject) value, schemaArray);
    } else {
      return JsonNull.INSTANCE;
    }
  }

  /**
   * Parses primitive types
   * @param schema
   * @param value
   * @return
   * @throws DataConversionException
   */
  private JsonElement parsePrimitiveType(JsonSchema schema, JsonElement value)
      throws DataConversionException {

    if ((schema.isType(NULL) || schema.isNullable()) && value.isJsonNull()) {
      return JsonNull.INSTANCE;
    }

    if ((schema.isType(NULL) && !value.isJsonNull()) || (!schema.isType(NULL) && value.isJsonNull())) {
      throw new DataConversionException(
          "Type mismatch for " + value.toString() + " of type " + schema.getDataTypes().toString());
    }

    if (schema.isType(FIXED)) {
      int expectedSize = schema.getSizeOfFixedData();
      if (value.getAsString().length() == expectedSize) {
        return value;
      } else {
        throw new DataConversionException(
            "Fixed type value is not same as defined value expected fieldsCount: " + expectedSize);
      }
    } else {
      return value;
    }
  }
}
