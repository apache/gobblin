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

import static org.apache.gobblin.converter.json.JsonStringToJsonIntermediateConverter.JsonUtils.*;

/**
 * Converts a json string to a {@link JsonObject}.
 */
public class JsonStringToJsonIntermediateConverter extends Converter<String, JsonArray, String, JsonObject> {

  private final static Logger log = LoggerFactory.getLogger(JsonStringToJsonIntermediateConverter.class);

  private static final String UNPACK_COMPLEX_SCHEMAS_KEY = "gobblin.converter.jsonStringToJsonIntermediate.unpackComplexSchemas";

  private boolean unpackComplexSchemas;

  private static final String TEMP_COLUMN_NAME = "temp";

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

  /**
   * Overloads parseJsonBasedOnSchema to support JsonArray based inputs
   * @param input
   * @param schema
   * @return
   * @throws DataConversionException
   */
  private JsonArray parseJsonBasedOnSchema(JsonArray input, JsonArray schema)
      throws DataConversionException {
    JsonObject firstSchema = schema.get(0).getAsJsonObject().get("dataType").getAsJsonObject();
    return unwrapTempAsArray(wrapAndProcess(input, firstSchema));
  }

  /**
   * Parses a provided JsonObject input using the provided JsonArray schema into
   * a JsonObject.
   * @param record
   * @param schema
   * @return
   * @throws DataConversionException
   */
  private JsonObject parseJsonBasedOnSchema(JsonObject record, JsonArray schema)
      throws DataConversionException {
    try {

      JsonObject output = new JsonObject();
      for (int i = 0; i < schema.size(); i++) {

        JsonElement schemaElement = schema.get(i);
        JsonObject schemaObject = schemaElement.getAsJsonObject();
        String expectedColumnName = getColumnName(schemaObject);
        JsonArray dataType = getDataTypeTypeFromSchema(schemaObject);

        if (record.has(expectedColumnName)) {

          JsonElement value = record.get(expectedColumnName);
          if (isUnionType(schemaObject)) {
            JsonArray unionSchema = buildUnionSchema(schemaElement);
            JsonObject firstTypeSchema = getFirstType(unionSchema).getAsJsonObject();
            JsonObject secondTypeSchema = getSecondType(unionSchema).getAsJsonObject();
            try {
              output.add(expectedColumnName, unwrapTemp(wrapAndProcess(value, firstTypeSchema)));
            } catch (Exception e) {
              output.add(expectedColumnName, unwrapTemp(wrapAndProcess(value, secondTypeSchema)));
            }
          } else if (isEnumType(schemaObject)) {
            JsonElement parseEnumType = parseEnumType(schemaElement, value);
            output.add(expectedColumnName, parseEnumType);
          } else if (value.isJsonArray()) {
            JsonElement parsed = parseJsonArrayType(schemaElement, value);
            output.add(expectedColumnName, parsed);
          } else if (value.isJsonObject()) {
            JsonElement parsed = parseJsonObjectType(schemaElement, value);
            output.add(expectedColumnName, parsed);
          } else {
            JsonElement parsedElement = parsePrimitiveType(schemaObject, dataType, value);
            output.add(expectedColumnName, parsedElement);
          }
        } else {
          output.add(expectedColumnName, JsonNull.INSTANCE);
        }
      }
      return output;
    } catch (Exception e) {
      e.printStackTrace();
      throw new DataConversionException("Unable to parse " + record.toString() + " with schema " + schema.toString());
    }
  }

  private JsonArray buildUnionSchema(JsonElement schemaElement) {
    JsonElement otherSchema = new JsonParser().parse(schemaElement.toString());
    JsonArray dataType = getDataTypeTypeFromSchema(schemaElement);
    JsonElement firstType = getFirstType(dataType);
    JsonElement secondType = getSecondType(dataType);
    if (firstType.isJsonObject()) {
      schemaElement.getAsJsonObject().add("dataType", firstType.getAsJsonObject().get("dataType").getAsJsonObject());
    } else {
      schemaElement.getAsJsonObject().get("dataType").getAsJsonObject().add("type", firstType.getAsJsonPrimitive());
    }
    if (secondType.isJsonObject()) {
      otherSchema.getAsJsonObject().add("dataType", secondType.getAsJsonObject().get("dataType").getAsJsonObject());
    } else {
      otherSchema.getAsJsonObject().get("dataType").getAsJsonObject().add("type", secondType.getAsJsonPrimitive());
    }
    JsonArray unionSchema = new JsonArray();
    unionSchema.add(schemaElement.getAsJsonObject().get("dataType").getAsJsonObject());
    unionSchema.add(otherSchema.getAsJsonObject().get("dataType").getAsJsonObject());
    return unionSchema;
  }

  /**
   * Parses Enum type values
   * @param schema
   * @param value
   * @return
   * @throws DataConversionException
   */
  private JsonElement parseEnumType(JsonElement schema, JsonElement value)
      throws DataConversionException {
    JsonObject schemaObject = schema.getAsJsonObject();
    JsonArray allowedSymbols = allowedSymbolsInEnum(schemaObject);
    if (allowedSymbols.contains(jsonArray(value).get(0))) {
      return value;
    } else {
      throw new DataConversionException(
          "Invalid symbol: " + value.getAsString() + " allowed values: " + allowedSymbols.toString());
    }
  }

  /**
   * Parses JsonArray type values
   * @param schema
   * @param value
   * @return
   * @throws DataConversionException
   */
  private JsonElement parseJsonArrayType(JsonElement schema, JsonElement value)
      throws DataConversionException {
    JsonObject schemaObject = schema.getAsJsonObject();
    String arrayType = arrayType(schema);
    JsonElement nestedType = schemaObject.get("dataType").getAsJsonObject().get("items");
    if (isPrimitiveType(arrayType)) {
      return value;
    } else if (isMapType(nestedType)) {
      JsonElement arrayItems = getItemsWithinDataType(getDataType(schemaObject));
      if (arrayItems.isJsonPrimitive() || arrayItems.isJsonNull()) {
        return value;
      } else if (arrayItems.isJsonObject()) {
        JsonArray tempArray = new JsonArray();
        JsonArray valueArray = value.getAsJsonArray();
        for (int index = 0; index < valueArray.size(); index++) {
          tempArray.add(unwrapTempAsObject(
              wrapAndProcess(valueArray.get(index).getAsJsonObject(), getDataType(arrayItems.getAsJsonObject()))));
        }
        return tempArray;
      }
      return new JsonArray();
    } else if (isRecordType(nestedType)) {
      JsonArray tempArray = new JsonArray();
      JsonArray valArray = value.getAsJsonArray();
      JsonArray schemaArr = getSchemaForArrayHavingRecord(schemaObject);
      for (int j = 0; j < schemaArr.size(); j++) {
        tempArray.add(parseJsonBasedOnSchema((JsonObject) valArray.get(j), schemaArr));
      }
      return tempArray;
    } else {
      JsonArray newArray = new JsonArray();
      for (JsonElement v : value.getAsJsonArray()) {
        newArray.add(parseJsonBasedOnSchema((JsonObject) v, jsonArray(schema)));
      }
      return new JsonArray();
    }
  }

  /**
   * Parses JsonObject type values
   * @param schema
   * @param value
   * @return
   * @throws DataConversionException
   */
  private JsonElement parseJsonObjectType(JsonElement schema, JsonElement value)
      throws DataConversionException {
    JsonObject schemaObject = schema.getAsJsonObject();
    if (isMapType(schema)) {
      if (getValuesWithinDataType(schemaObject).isJsonPrimitive()) {
        return value;
      } else if (getValuesWithinDataType(schemaObject).isJsonObject()) {
        JsonObject mapValueSchema = getValuesWithinDataType(schemaObject).getAsJsonObject();

        JsonObject map = new JsonObject();
        for (Entry<String, JsonElement> mapEntry : value.getAsJsonObject().entrySet()) {
          JsonElement mapValue = mapEntry.getValue();
          if (mapValue.isJsonArray()) {
            map.add(mapEntry.getKey(), parseJsonBasedOnSchema(mapValue.getAsJsonArray(), jsonArray(mapValueSchema)));
          } else if (mapValue.isJsonObject()) {
            map.add(mapEntry.getKey(),
                unwrapTempAsObject(wrapAndProcess(mapValue.getAsJsonObject(), getDataType(mapValueSchema))));
          } else {
            map.add(mapEntry.getKey(),
                unwrapTempAsPrimitive(wrapAndProcess(mapValue.getAsJsonPrimitive(), getDataType(mapValueSchema))));
          }
        }
        return map;
      } else {
        return value;
      }
    } else if (isRecordType(schema)) {
      JsonArray schemaArray = getValuesWithinDataType(schemaObject).getAsJsonArray();
      return parseJsonBasedOnSchema((JsonObject) value, schemaArray);
    } else {
      return JsonNull.INSTANCE;
    }
  }

  /**
   * Parses primitive types
   * @param schemaObject
   * @param dataType
   * @param value
   * @return
   * @throws DataConversionException
   */
  private JsonElement parsePrimitiveType(JsonObject schemaObject, JsonArray dataType, JsonElement value)
      throws DataConversionException {
    if (isNullType(schemaObject) && value.isJsonNull()) {
      return JsonNull.INSTANCE;
    }
    if ((isNullType(schemaObject) && !value.isJsonNull()) || (!isNullType(schemaObject) && value.isJsonNull())) {
      throw new DataConversionException(
          "Type mismatch for " + value.toString() + " of type " + getDataTypeTypeFromSchema(schemaObject).toString());
    }

    if (isFixedType(schemaObject)) {
      int expectedSize = getSizeOfFixedData(schemaObject);
      if (value.getAsString().length() == expectedSize) {
        return value;
      } else {
        throw new DataConversionException(
            "Fixed type value is not same as defined value expected size: " + expectedSize);
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
  private JsonObject wrapAndProcess(JsonElement input, JsonObject schema)
      throws DataConversionException {
    return parseJsonBasedOnSchema(buildTempObject(input), buildTempSchema(schema));
  }

  /**
   * Get the original value back from temp object as object
   * @param parsed
   * @return
   */
  private JsonElement unwrapTempAsObject(JsonObject parsed) {
    return parsed.get(TEMP_COLUMN_NAME).getAsJsonObject();
  }

  /**
   * Get the original value back from temp object as primitive
   * @param parsed
   * @return
   */
  private JsonElement unwrapTempAsPrimitive(JsonObject parsed) {
    return parsed.get(TEMP_COLUMN_NAME).getAsJsonPrimitive();
  }

  /**
   * Get the original value back from temp object as an array
   * @param object
   * @return
   */
  private JsonArray unwrapTempAsArray(JsonObject object) {
    return object.get(TEMP_COLUMN_NAME).getAsJsonArray();
  }

  private JsonElement unwrapTemp(JsonObject jsonObject) {
    JsonElement element = jsonObject.get(TEMP_COLUMN_NAME);
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
      tempObject.add(TEMP_COLUMN_NAME, prop.getAsJsonObject());
    } else if (prop instanceof JsonArray) {
      tempObject.add(TEMP_COLUMN_NAME, prop.getAsJsonArray());
    } else if (prop instanceof JsonPrimitive) {
      tempObject.add(TEMP_COLUMN_NAME, prop.getAsJsonPrimitive());
    } else if (prop instanceof JsonNull) {
      tempObject.add(TEMP_COLUMN_NAME, prop.getAsJsonNull());
    }
    return tempObject;
  }

  /**
   * Wraps the schema within dataType and add columnName temp
   * @param dataType
   * @return
   */
  private JsonArray buildTempSchema(JsonObject dataType) {
    JsonObject schemaObj = new JsonObject();
    schemaObj.addProperty("columnName", TEMP_COLUMN_NAME);
    schemaObj.add("dataType", dataType);
    return jsonArray(schemaObj);
  }

  public static class JsonUtils {
    /**
     * Fetches dataType.values from the JsonObject
     * @param schemaObject
     * @return
     */
    public static JsonElement getValuesWithinDataType(JsonObject schemaObject) {
      return getDataType(schemaObject).get("values");
    }

    /**
     * Gets size for fixed type viz dataType.size from the JsonObject
     * @param schemaObject
     * @return
     */
    public static int getSizeOfFixedData(JsonObject schemaObject) {
      return getDataType(schemaObject).get("size").getAsInt();
    }

    /**
     * Gets schema of record which is present within array schema.
     * @param schemaObject
     * @return
     */
    public static JsonArray getSchemaForArrayHavingRecord(JsonObject schemaObject) {
      return getValuesWithinDataType(getItemsWithinDataType(getDataType(schemaObject)).getAsJsonObject())
          .getAsJsonArray();
    }

    /**
     * Gets allowed symbols in enum from schema of enum type
     * @param schemaObject
     * @return
     */
    public static JsonArray allowedSymbolsInEnum(JsonObject schemaObject) {
      return getDataType(schemaObject).get("symbols").getAsJsonArray();
    }

    public static boolean isEnumType(JsonObject schemaObject) {
      return getDataTypeTypeFromSchema(schemaObject).get(0).getAsString().equalsIgnoreCase("enum");
    }

    /**
     * Get dataType.type from schema
     * @param schemaObject
     * @return
     */
    public static JsonArray getDataTypeTypeFromSchema(JsonObject schemaObject) {
      JsonObject dataType = getDataType(schemaObject);
      return dataType.has("type") ? dataType.get("type").isJsonPrimitive() ? jsonArray(dataType.get("type"))
          : (dataType.get("type").isJsonArray() ? dataType.get("type").getAsJsonArray() : null) : null;
    }

    /**
     * Get dataType.type from schema
     * @param schemaElement
     * @return
     */
    public static JsonArray getDataTypeTypeFromSchema(JsonElement schemaElement) {
      return getDataTypeTypeFromSchema(schemaElement.getAsJsonObject());
    }

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

    public static String getColumnName(JsonObject schemaObject) {
      return schemaObject.get("columnName").getAsString();
    }

    public static boolean isMapType(JsonElement schema) {
      return isTypeEqual(schema, "map");
    }

    public static boolean isRecordType(JsonElement schema) {
      return isTypeEqual(schema, "record");
    }

    public static boolean isUnionType(JsonObject schemaObject) {
      JsonArray type = getDataTypeTypeFromSchema(schemaObject);
      return (type != null ? type.size() : 0) == 2;
    }

    public static boolean isPrimitiveType(String arrayType) {
      return arrayType != null && "null boolean int long float double bytes string enum fixed"
          .contains(arrayType.toLowerCase());
    }

    public static boolean isFixedType(JsonElement schema) {
      return isTypeEqual(schema, "fixed");
    }

    public static boolean isArrayType(JsonElement schema) {
      return isTypeEqual(schema, "array");
    }

    public static boolean isTypeEqual(JsonElement schema, String expectedType) {
      JsonArray type = getDataTypeTypeFromSchema(schema);
      return type.get(0).getAsString().equalsIgnoreCase(expectedType);
    }

    public static boolean isNullType(JsonElement schemeElement) {
      return isTypeEqual(schemeElement, "null");
    }

    /**
     * Fetches the nested or primitive array items type from schema.
     * @param schema
     * @return
     * @throws DataConversionException
     */
    public static String arrayType(JsonElement schema)
        throws DataConversionException {
      if (!isArrayType(schema)) {
        return null;
      }

      JsonElement arrayValues = getItemsWithinDataType(getDataType(schema.getAsJsonObject()));

      try {
        return arrayValues.isJsonPrimitive() && arrayValues.getAsJsonPrimitive().isString() ? arrayValues.getAsString()
            : getDataTypeTypeFromSchema(arrayValues.getAsJsonObject()).get(0).getAsString();
      } catch (UnsupportedOperationException | IllegalStateException e) {
        //values is not string and a nested json array
        throw new DataConversionException("Array types only allow values as primitive, null or JsonObject");
      }
    }

    public static JsonElement getItemsWithinDataType(JsonObject object) {
      return object.get("items");
    }

    public static JsonObject getDataType(JsonObject element) {
      return element.get("dataType").getAsJsonObject();
    }

    public static String getPropOrBlankString(JsonObject map, String key) {
      return map.has(key) ? map.get(key).getAsString() : "";
    }

    public static boolean getBooleanIfExists(JsonObject map, String key) {
      return map.has(key) && map.get(key).getAsBoolean();
    }

    public static JsonElement getType(JsonArray jsonArray) {
      return getFirstType(jsonArray);
    }

    public static JsonElement getFirstType(JsonArray jsonArray) {
      return jsonArray.get(0);
    }

    public static JsonElement getSecondType(JsonArray jsonArray) {
      return jsonArray.get(1);
    }
  }
}
