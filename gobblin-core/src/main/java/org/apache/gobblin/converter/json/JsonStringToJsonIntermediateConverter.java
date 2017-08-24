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

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * Converts a json string to a {@link JsonObject}.
 */
public class JsonStringToJsonIntermediateConverter extends Converter<String, JsonArray, String, JsonObject> {

  private final static Logger log = LoggerFactory.getLogger(JsonStringToJsonIntermediateConverter.class);

  private static final String UNPACK_COMPLEX_SCHEMAS_KEY = "gobblin.converter.jsonStringToJsonIntermediate.unpackComplexSchemas";

  private boolean unpackComplexSchemas;

  /**
   * Take in an input schema of type string, the schema must be in JSON format
   * @return a JsonArray representation of the schema
   */
  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    this.unpackComplexSchemas = workUnit.getPropAsBoolean(UNPACK_COMPLEX_SCHEMAS_KEY, true);

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
    JsonObject rec = parseJsonBasedOnSchema(inputRecord, outputSchema);
    return new SingleRecordIterable(rec);
  }

  private JsonObject parseJsonBasedOnSchema(JsonObject input, JsonArray fields)
      throws DataConversionException {
    try {

      JsonObject output = new JsonObject();
      for (int i = 0; i < fields.size(); i++) {

        JsonElement schemaElement = fields.get(i);
        JsonObject schemaObject = schemaElement.getAsJsonObject();
        String expectedColumnName = getExpectedColumnName(schemaObject);
        String type = getType(schemaObject);

        if (input.has(expectedColumnName)) {

          JsonElement value = input.get(expectedColumnName);
          if (isEnumType(schemaObject)) {
            JsonArray allowedSymbols = allowedSymbolsInEnum(schemaObject);
            if (allowedSymbols.contains(createJsonElementArray(value).get(0))) {
              output.add(expectedColumnName, value);
            } else {
              throw new DataConversionException(
                  "Invalid symbol: " + value.getAsString() + " allowed values: " + allowedSymbols.toString());
            }
          } else if (value.isJsonArray()) {
            //value is json Array now verify if schemaElement permits this
            String arrayType = arrayType(schemaElement);
            if (isPrimitiveArrayType(arrayType)) {
              output.add(expectedColumnName, value);
            } else if (isArrayType(arrayType, "map")) {
              output.add(expectedColumnName, value);
            } else if (isArrayType(arrayType, "record")) {
              JsonArray tempArray = new JsonArray();
              JsonArray valArray = value.getAsJsonArray();
              JsonArray schemaArr = getSchemaForArrayHavingRecord(schemaObject);
              for (int j = 0; j < schemaArr.size(); j++) {
                tempArray.add(parseJsonBasedOnSchema((JsonObject) valArray.get(j), schemaArr));
              }
              output.add(expectedColumnName, tempArray);
            } else {
              JsonArray newArray = new JsonArray();
              for (JsonElement v : value.getAsJsonArray()) {
                newArray.add(parseJsonBasedOnSchema((JsonObject) v, createJsonElementArray(schemaElement)));
              }
              output.add(expectedColumnName, new JsonArray());
            }
          } else if (value.isJsonObject()) {
            if (isMapType(schemaElement)) {
              output.add(expectedColumnName, value);
            } else if (isRecordType(schemaElement)) {
              JsonArray schemaArray = getValuesFromDataType(schemaObject);
              output.add(expectedColumnName, parseJsonBasedOnSchema((JsonObject) value, schemaArray));
            } else {
              output.add(expectedColumnName, JsonNull.INSTANCE);
            }
          } else {
            if (type.equalsIgnoreCase("fixed")) {
              int expectedSize = getSizeOfFixedData(schemaObject);
              if (value.getAsString().length() == expectedSize) {
                output.add(expectedColumnName, value);
              } else {
                throw new DataConversionException(
                    "Fixed type value is not same as defined value: " + value.toString() + " expected size: "
                        + expectedSize);
              }
            } else {
              output.add(expectedColumnName, value);
            }
          }
        } else {
          output.add(expectedColumnName, JsonNull.INSTANCE);
        }
      }
      return output;
    } catch (Exception e) {
      e.printStackTrace();
      throw new DataConversionException("Unable to parse " + input.toString() + " with schema " + fields.toString());
    }
  }

  private int getSizeOfFixedData(JsonObject schemaObject) {
    return schemaObject.get("dataType").getAsJsonObject().get("size").getAsInt();
  }

  private JsonArray getSchemaForArrayHavingRecord(JsonObject schemaObject) {
    return schemaObject.get("dataType").getAsJsonObject().get("items").getAsJsonObject().get("dataType")
        .getAsJsonObject().get("values").getAsJsonArray();
  }

  private JsonArray allowedSymbolsInEnum(JsonObject schemaObject) {
    return schemaObject.get("dataType").getAsJsonObject().get("symbols").getAsJsonArray();
  }

  private boolean isEnumType(JsonObject schemaObject) {
    return getDataTypeFromSchema(schemaObject).equalsIgnoreCase("enum");
  }

  private String getDataTypeFromSchema(JsonObject schemaObject) {
    return schemaObject.get("dataType").getAsJsonObject().get("type").getAsString();
  }

  private String getDataTypeFromSchema(JsonElement schemaElement) {
    return schemaElement.getAsJsonObject().get("dataType").getAsJsonObject().get("type").getAsString();
  }

  private JsonArray getValuesFromDataType(JsonObject schemaObject) {
    return schemaObject.get("dataType").getAsJsonObject().get("values").getAsJsonArray();
  }

  private JsonArray createJsonElementArray(JsonElement element) {
    JsonArray temp = new JsonArray();
    temp.add(element);
    return temp;
  }

  private boolean isArrayType(String arrayType, String type) {
    return arrayType != null && arrayType.equalsIgnoreCase(type);
  }

  private String getType(JsonObject schemaObject) {
    if (schemaObject.has("dataType")) {
      if (schemaObject.get("dataType").getAsJsonObject().has("type")) {
        return getDataTypeFromSchema(schemaObject);
      }
      return "";
    }
    return "";
  }

  private String getExpectedColumnName(JsonObject schemaObject) {
    return schemaObject.has("columnName") ? schemaObject.get("columnName").getAsString() : "";
  }

  private boolean isMapType(JsonElement field) {
    return getDataTypeFromSchema(field).equalsIgnoreCase("map");
  }

  private boolean isRecordType(JsonElement field) {
    return getDataTypeFromSchema(field).equalsIgnoreCase("record");
  }

  private boolean isPrimitiveArrayType(String arrayType) {
    return arrayType != null && "null boolean int long float double bytes string enum fixed"
        .contains(arrayType.toLowerCase());
  }

  private String arrayType(JsonElement arraySchema) {
    String arrayType = getDataTypeFromSchema(arraySchema);
    boolean isArray = arrayType.equalsIgnoreCase("array");
    JsonElement arrayValues = arraySchema.getAsJsonObject().get("dataType").getAsJsonObject().get("items");
    try {
      return isArray ? arrayValues.getAsString() : null;
    } catch (UnsupportedOperationException | IllegalStateException e) {
      //values is not string and a nested json array
      return isArray ? getDataTypeFromSchema(arrayValues.getAsJsonObject()) : null;
    }
  }
}
