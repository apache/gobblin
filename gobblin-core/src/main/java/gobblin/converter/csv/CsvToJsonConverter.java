/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter.csv;

import gobblin.converter.Converter;
import gobblin.converter.SingleRecordIterable;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.source.extractor.utils.InputStreamCSVReader;


public class CsvToJsonConverter extends Converter<String, JsonArray, String, JsonObject> {
  private static final String NULL = "null";

  /**
   * Take in an input schema of type string, the schema must be in JSON format
   * @return a JsonArray representation of the schema
   */
  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    JsonParser jsonParser = new JsonParser();
    JsonElement jsonSchema = jsonParser.parse(inputSchema);
    return jsonSchema.getAsJsonArray();
  }

  /**
   * Takes in a record with format String and splits the data based on SOURCE_SCHEMA_DELIMITER
   * Uses the inputSchema and the split record to convert the record to a JsonObject
   * @return a JsonObject representing the record
   * @throws IOException
   */
  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, String inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    String strDelimiter = workUnit.getProp(ConfigurationKeys.CONVERTER_CSV_TO_JSON_DELIMITER);
    if (Strings.isNullOrEmpty(strDelimiter)) {
      throw new IllegalArgumentException("Delimiter cannot be empty");
    }
    InputStreamCSVReader reader =
        new InputStreamCSVReader(inputRecord, strDelimiter.charAt(0), workUnit.getProp(
            ConfigurationKeys.CONVERTER_CSV_TO_JSON_ENCLOSEDCHAR,
            ConfigurationKeys.DEFAULT_CONVERTER_CSV_TO_JSON_ENCLOSEDCHAR).charAt(0));
    List<String> recordSplit;
    try {
      recordSplit = Lists.newArrayList(reader.splitRecord());
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
    JsonObject outputRecord = new JsonObject();

    for (int i = 0; i < outputSchema.size(); i++) {
      if (i < recordSplit.size()) {
        if (recordSplit.get(i) == null) {
          outputRecord.add(outputSchema.get(i).getAsJsonObject().get("columnName").getAsString(), JsonNull.INSTANCE);
        } else if (recordSplit.get(i).isEmpty() || recordSplit.get(i).toLowerCase().equals(NULL)) {
          outputRecord.add(outputSchema.get(i).getAsJsonObject().get("columnName").getAsString(), JsonNull.INSTANCE);
        } else {
          outputRecord
              .addProperty(outputSchema.get(i).getAsJsonObject().get("columnName").getAsString(), recordSplit.get(i));
        }
      } else {
        outputRecord.add(outputSchema.get(i).getAsJsonObject().get("columnName").getAsString(), JsonNull.INSTANCE);
      }
    }

    return new SingleRecordIterable<JsonObject>(outputRecord);
  }
}
