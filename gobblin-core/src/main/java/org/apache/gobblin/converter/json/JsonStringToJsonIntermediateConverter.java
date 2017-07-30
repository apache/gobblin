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

package gobblin.converter.json;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
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
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
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

    JsonObject outputRecord = new JsonObject();

    for (int i = 0; i < outputSchema.size(); i++) {
      String expectedColumnName = outputSchema.get(i).getAsJsonObject().get("columnName").getAsString();

      if (inputRecord.has(expectedColumnName)) {
        //As currently gobblin.converter.avro.JsonIntermediateToAvroConverter is not able to handle complex schema's so storing it as string

        if (inputRecord.get(expectedColumnName).isJsonArray()) {
          outputRecord.addProperty(expectedColumnName, inputRecord.get(expectedColumnName).toString());
        } else if (inputRecord.get(expectedColumnName).isJsonObject()) {
          //To check if internally in an JsonObject there is multiple hierarchy
          boolean isMultiHierarchyInsideJsonObject = false;
          for (Map.Entry<String, JsonElement> entry : ((JsonObject) inputRecord.get(expectedColumnName)).entrySet()) {
            if (entry.getValue().isJsonArray() || entry.getValue().isJsonObject()) {
              isMultiHierarchyInsideJsonObject = true;
              break;
            }
          }
          if (isMultiHierarchyInsideJsonObject) {
            outputRecord.addProperty(expectedColumnName, inputRecord.get(expectedColumnName).toString());
          } else {
            outputRecord.add(expectedColumnName, inputRecord.get(expectedColumnName));
          }

        } else {
          outputRecord.add(expectedColumnName, inputRecord.get(expectedColumnName));
        }
      } else {
        outputRecord.add(expectedColumnName, JsonNull.INSTANCE);
      }

    }
    return new SingleRecordIterable<>(outputRecord);
  }
}
