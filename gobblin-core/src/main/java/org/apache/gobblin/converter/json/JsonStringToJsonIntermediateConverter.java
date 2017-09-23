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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import static org.apache.gobblin.converter.json.JsonSchema.DATA_TYPE_KEY;
import static org.apache.gobblin.converter.json.JsonSchema.DEFAULT_RECORD_COLUMN_NAME;
import static org.apache.gobblin.converter.json.JsonSchema.InputType;
import static org.apache.gobblin.converter.json.JsonStringToJsonIntermediateConverter.JsonUtils.jsonArray;


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
    JsonObject rec = parseJsonBasedOnSchema(inputRecord, schema);
    return new SingleRecordIterable(rec);
  }

  /**
   * Overloads parseJsonBasedOnSchema to support JsonArray based inputs
   * @param input
   * @param schema
   * @return
   * @throws DataConversionException
   */
  private JsonArray parseJsonBasedOnSchema(JsonArray input, JsonArray schema)
      throws DataConversionException {
    JsonObject firstSchema = schema.get(0).getAsJsonObject();
    return unwrapTempAsArray(wrapAndProcess(input, JsonSchema.buildBaseSchemaByWrap(firstSchema)));
  }

  /**
   * Parses a provided JsonObject input using the provided JsonArray schema into
   * a JsonObject.
   * @param record
   * @param schema
   * @return
   * @throws DataConversionException
   */
  private JsonObject parseJsonBasedOnSchema(JsonObject record, JsonSchema schema)
      throws DataConversionException {
    try {

      JsonObject output = new JsonObject();
      for (int i = 0; i < schema.fieldsCount(); i++) {
        JsonSchema schemaElement = schema.getFieldSchemaAt(i);
        String columnKey = schemaElement.getColumnName();
        if (record.has(columnKey)) {
          JsonElement columnValue = record.get(columnKey);
          if (schemaElement.isUnionType()) {
            JsonSchema firstTypeSchema = schemaElement.getFirstTypeSchema();
            JsonSchema secondTypeSchema = schemaElement.getSecondTypeSchema();
            try {
              output.add(columnKey, unwrapTemp(wrapAndProcess(columnValue, firstTypeSchema)));
            } catch (DataConversionException e) {
              output.add(columnKey, unwrapTemp(wrapAndProcess(columnValue, secondTypeSchema)));
            }
          } else if (schemaElement.isEnumType()) {
            JsonElement parseEnumType = parseEnumType(schemaElement, columnValue);
            output.add(columnKey, parseEnumType);
          } else if (columnValue.isJsonArray()) {
            JsonElement parsed = parseJsonArrayType(schemaElement, columnValue);
            output.add(columnKey, parsed);
          } else if (columnValue.isJsonObject()) {
            JsonElement parsed = parseJsonObjectType(schemaElement, columnValue);
            output.add(columnKey, parsed);
          } else {
            JsonElement parsedElement = parsePrimitiveType(schemaElement, columnValue);
            output.add(columnKey, parsedElement);
          }
        } else {
          output.add(columnKey, JsonNull.INSTANCE);
        }
      }
      return output;
    } catch (Exception e) {
      e.printStackTrace();
      throw new DataConversionException("Unable to parse " + record.toString() + " with schema " + schema.toString());
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
    if (schema.getSymbols().contains(jsonArray(value).get(0))) {
      return value;
    } else {
      throw new DataConversionException(
          "Invalid symbol: " + value.getAsString() + " allowed values: " + schema.getSymbols().toString());
    }
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
    InputType arrayType = schema.arrayType();
    JsonElement nestedItem = schema.getItemsWithinDataType();
    JsonSchema nestedSchema = null;
    if (nestedItem.isJsonObject()) {
      nestedSchema = new JsonSchema(nestedItem.getAsJsonObject());
    }
    if (nestedItem.isJsonArray()) {
      nestedSchema = new JsonSchema(nestedItem.getAsJsonArray());
    }
    if (InputType.primitiveTypes.contains(arrayType)) {
      return value;
    } else if (nestedSchema.isMapType()) {
      JsonArray tempArray = new JsonArray();
      JsonArray valueArray = value.getAsJsonArray();
      for (int index = 0; index < valueArray.size(); index++) {
        tempArray.add(unwrapTempAsObject(
            wrapAndProcess(valueArray.get(index).getAsJsonObject(), new JsonSchema(nestedItem.getAsJsonObject()))));
      }
      return tempArray;
    } else if (nestedSchema.isRecordType()) {
      JsonArray tempArray = new JsonArray();
      JsonArray valArray = value.getAsJsonArray();
      JsonArray schemaArr = schema.getSchemaForArrayHavingRecord();
      for (int j = 0; j < schemaArr.size(); j++) {
        tempArray.add(parseJsonBasedOnSchema((JsonObject) valArray.get(j), new JsonSchema(schemaArr)));
      }
      return tempArray;
    } else {
      JsonArray newArray = new JsonArray();
      for (JsonElement v : value.getAsJsonArray()) {
        newArray.add(parseJsonBasedOnSchema((JsonObject) v, schema));
      }
      return new JsonArray();
    }
  }

  /**
   * Parses JsonObject type values
   * @param value
   * @return
   * @throws DataConversionException
   */
  private JsonElement parseJsonObjectType(JsonSchema schema, JsonElement value)
      throws DataConversionException {
    JsonElement valuesWithinDataType = schema.getValuesWithinDataType();
    if (schema.isMapType()) {
      if (valuesWithinDataType.isJsonPrimitive()) {
        return value;
      } else if (valuesWithinDataType.isJsonObject()) {
        JsonObject mapValueSchema = valuesWithinDataType.getAsJsonObject().get(DATA_TYPE_KEY).getAsJsonObject();

        JsonObject map = new JsonObject();
        for (Entry<String, JsonElement> mapEntry : value.getAsJsonObject().entrySet()) {
          JsonElement mapValue = mapEntry.getValue();
          if (mapValue.isJsonArray()) {
            map.add(mapEntry.getKey(), parseJsonBasedOnSchema(mapValue.getAsJsonArray(), jsonArray(mapValueSchema)));
          } else {
            JsonSchema schema1 = JsonSchema.buildBaseSchemaByWrap(mapValueSchema);
            if (mapValue.isJsonObject()) {
              map.add(mapEntry.getKey(), unwrapTempAsObject(wrapAndProcess(mapValue.getAsJsonObject(), schema1)));
            } else {
              map.add(mapEntry.getKey(), unwrapTempAsPrimitive(wrapAndProcess(mapValue.getAsJsonPrimitive(), schema1)));
            }
          }
        }
        return map;
      } else {
        return value;
      }
    } else if (schema.isRecordType()) {
      JsonArray schemaArray = valuesWithinDataType.getAsJsonArray();
      return parseJsonBasedOnSchema((JsonObject) value, new JsonSchema(schemaArray));
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

    if (schema.isNullType() && value.isJsonNull()) {
      return JsonNull.INSTANCE;
    }
    if ((schema.isNullType() && !value.isJsonNull()) || (!schema.isNullType() && value.isJsonNull())) {
      throw new DataConversionException(
          "Type mismatch for " + value.toString() + " of type " + schema.getDataTypes().toString());
    }

    if (schema.isFixedType()) {
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

  /**
   * Wraps input and schema into respective temp objects and parseJsonBasedSchema
   * @param input
   * @param schema
   * @return
   * @throws DataConversionException
   */
  private JsonObject wrapAndProcess(JsonElement input, JsonSchema schema)
      throws DataConversionException {
    return parseJsonBasedOnSchema(buildTempObject(input), schema);
  }

  /**
   * Get the original value back from temp object as object
   * @param parsed
   * @return
   */
  private JsonElement unwrapTempAsObject(JsonObject parsed) {
    return parsed.get(DEFAULT_RECORD_COLUMN_NAME).getAsJsonObject();
  }

  /**
   * Get the original value back from temp object as primitive
   * @param parsed
   * @return
   */
  private JsonElement unwrapTempAsPrimitive(JsonObject parsed) {
    return parsed.get(DEFAULT_RECORD_COLUMN_NAME).getAsJsonPrimitive();
  }

  /**
   * Get the original value back from temp object as an array
   * @param object
   * @return
   */
  private JsonArray unwrapTempAsArray(JsonObject object) {
    return object.get(DEFAULT_RECORD_COLUMN_NAME).getAsJsonArray();
  }

  private JsonElement unwrapTemp(JsonObject jsonObject) {
    JsonElement element = jsonObject.get(DEFAULT_RECORD_COLUMN_NAME);
    if (element.isJsonArray()) {
      return element.getAsJsonArray();
    }
    if (element.isJsonPrimitive()) {
      return element.getAsJsonPrimitive();
    }
    if (element.isJsonObject()) {
      return element.getAsJsonObject();
    }
    if (element.isJsonNull()) {
      return element.getAsJsonNull();
    }
    return null;
  }

  /**
   * Wraps the object into a JsonObject with key temp
   * @param prop
   * @return
   */
  private JsonObject buildTempObject(JsonElement prop) {
    JsonObject tempObject = new JsonObject();
    if (prop instanceof JsonObject) {
      tempObject.add(DEFAULT_RECORD_COLUMN_NAME, prop.getAsJsonObject());
    } else if (prop instanceof JsonArray) {
      tempObject.add(DEFAULT_RECORD_COLUMN_NAME, prop.getAsJsonArray());
    } else if (prop instanceof JsonPrimitive) {
      tempObject.add(DEFAULT_RECORD_COLUMN_NAME, prop.getAsJsonPrimitive());
    } else if (prop instanceof JsonNull) {
      tempObject.add(DEFAULT_RECORD_COLUMN_NAME, prop.getAsJsonNull());
    }
    return tempObject;
  }

  public static class JsonUtils {

    /**
     * Creates a {@link JsonArray} with one {@link JsonElement}
     * @param element
     * @return
     */
    public static JsonArray jsonArray(JsonElement element) {
      JsonArray temp = new JsonArray();
      temp.add(element);
      return temp;
    }
  }
}
