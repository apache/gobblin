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
package org.apache.gobblin.converter.grok;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.DatasetFilterUtils;


/**
 * GrokToJsonConverter accepts already deserialized text row, String, where you can use.
 *
 * Converts Text to JSON based on Grok pattern. Schema is represented by the form of JsonArray same interface being used by CsvToJonConverter.
 * Each text record is represented by a String.
 * The converter only supports Grok patterns where every group is named because it uses the group names as column names.
 *
 * The following config properties can be set:
 * The grok pattern to use for the conversion:
 * converter.grokToJsonConverter.grokPattern ="^%{IPORHOST:clientip} (?:-|%{USER:ident}) (?:-|%{USER:auth}) \[%{HTTPDATE:timestamp}\] \"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|-)\" %{NUMBER:response} (?:-|%{NUMBER:bytes})"
 *
 * Path to the file which contains the base grok patterns which can be used in the converter's GROK pattern (if not set it will use the default ones):
 * converter.grokToJsonConverter.baseGrokPatternsFile=
 **
 * Specify a comma separated list of regexes which will be applied on the fields and matched one will be converted to json null:
 * converter.grokToJsonConverter.nullStringRegexes="[-\s]"
 *
 * Example of schema:
 * [
 {
 "columnName": "Day",
 "comment": "",
 "isNullable": "true",
 "dataType": {
 "type": "string"
 }
 },
 {
 "columnName": "Pageviews",
 "comment": "",
 "isNullable": "true",
 "dataType": {
 "type": "long"
 }
 }
 ]
 */
public class GrokToJsonConverter extends Converter<String, JsonArray, String, JsonObject> {

  private static final Logger LOG = LoggerFactory.getLogger(GrokToJsonConverter.class);
  private static final JsonParser JSON_PARSER = new JsonParser();
  private static final String COLUMN_NAME_KEY = "columnName";
  private static final String DATA_TYPE = "dataType";
  private static final String TYPE_KEY = "type";
  private static final String NULLABLE = "isNullable";

  public static final String GROK_PATTERN = "converter.grokToJsonConverter.grokPattern";
  public static final String BASE_PATTERNS_FILE = "converter.grokToJsonConverter.baseGrokPatternsFile";
  public static final String NULLSTRING_REGEXES = "converter.grokToJsonConverter.nullStringRegexes";

  public static final String DEFAULT_GROK_PATTERNS_FILE = "/grok/grok-patterns";

  private List<Pattern> nullStringRegexes;

  private Grok grok;

  @Override
  public Converter<String, JsonArray, String, JsonObject> init(WorkUnitState workUnit) {
    super.init(workUnit);
    String pattern = workUnit.getProp(GROK_PATTERN);
    String patternsFile = workUnit.getProp(BASE_PATTERNS_FILE);
    this.nullStringRegexes = DatasetFilterUtils.getPatternsFromStrings(workUnit.getPropAsList(NULLSTRING_REGEXES, ""));

    InputStreamReader grokPatterns;
    try {
      if (patternsFile == null) {
        grokPatterns = new InputStreamReader(getClass().getResourceAsStream("/grok/grok-base-patterns"), "UTF8");
      } else {
        grokPatterns = new InputStreamReader(new FileInputStream(patternsFile), "UTF8");
      }
      grok = new Grok();
      grok.addPatternFromReader(grokPatterns);
      grok.compile(pattern);
    } catch (GrokException | FileNotFoundException | UnsupportedEncodingException e) {
      throw new RuntimeException("Error initializing GROK: " + e);
    }

    return this;
  }

  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    Preconditions.checkNotNull(inputSchema, "inputSchema is required.");
    return JSON_PARSER.parse(inputSchema).getAsJsonArray();
  }

  /**
   * Converts Text (String) to JSON based on a Grok regexp expression.
   * By default, fields between Text and JSON are mapped by Grok SEMANTIC which is the identifier you give to the piece of text being matched in your Grok expression.
   *
   *
   * e.g:
   * {@inheritDoc}
   * @see Converter#convertRecord(Object, Object, WorkUnitState)
   */
  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, String inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    JsonObject outputRecord = createOutput(outputSchema, inputRecord);

    LOG.debug("Converted into " + outputRecord);

    return new SingleRecordIterable<JsonObject>(outputRecord);
  }

  @VisibleForTesting
  JsonObject createOutput(JsonArray outputSchema, String inputRecord)
      throws DataConversionException {
    JsonObject outputRecord = new JsonObject();

    Match gm = grok.match(inputRecord);
    gm.captures();

    JsonElement capturesJson = JSON_PARSER.parse(gm.toJson());

    for (JsonElement anOutputSchema : outputSchema) {
      JsonObject outputSchemaJsonObject = anOutputSchema.getAsJsonObject();
      String key = outputSchemaJsonObject.get(COLUMN_NAME_KEY).getAsString();
      String type = outputSchemaJsonObject.getAsJsonObject(DATA_TYPE).get(TYPE_KEY).getAsString();

      if (isFieldNull(capturesJson, key)) {
        if (!outputSchemaJsonObject.get(NULLABLE).getAsBoolean()) {
          throw new DataConversionException(
              "Field " + key + " is null or not exists but it is non-nullable by the schema.");
        }
        outputRecord.add(key, JsonNull.INSTANCE);
      } else {
        JsonElement jsonElement = capturesJson.getAsJsonObject().get(key);
        switch (type) {
          case "int":
            outputRecord.addProperty(key, jsonElement.getAsInt());
            break;
          case "long":
            outputRecord.addProperty(key, jsonElement.getAsLong());
            break;
          case "double":
            outputRecord.addProperty(key, jsonElement.getAsDouble());
            break;
          case "float":
            outputRecord.addProperty(key, jsonElement.getAsFloat());
            break;
          case "boolean":
            outputRecord.addProperty(key, jsonElement.getAsBoolean());
            break;
          case "string":
          default:
            outputRecord.addProperty(key, jsonElement.getAsString());
        }
      }
    }
    return outputRecord;
  }

  private boolean isFieldNull(JsonElement capturesJson, String key) {
    JsonObject jsonObject = capturesJson.getAsJsonObject();

    if (!jsonObject.has(key)) {
      return true;
    }

    for (Pattern pattern : this.nullStringRegexes) {
      if (pattern.matcher(jsonObject.get(key).getAsString()).matches()) {
        return true;
      }
    }

    return false;
  }
}
