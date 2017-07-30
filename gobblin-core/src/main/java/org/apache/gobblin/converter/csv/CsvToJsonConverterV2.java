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
package gobblin.converter.csv;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

/**
 * CsvToJsonConverterV2 accepts already deserialized (parsed) CSV row, String[], where you can use
 * @see CsvFileDownloader that conforms with RFC 4180 by leveraging Open CSV.
 *
 * Converts CSV to JSON. CSV schema is represented by the form of JsonArray same interface being used by CsvToJonConverter.
 * Each CSV record is represented by a array of String.
 *
 * Example of CSV schema:
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
public class CsvToJsonConverterV2 extends Converter<String, JsonArray, String[], JsonObject>  {

  private static final Logger LOG = LoggerFactory.getLogger(CsvToJsonConverterV2.class);
  private static final JsonParser JSON_PARSER = new JsonParser();
  private static final String COLUMN_NAME_KEY = "columnName";
  private static final String JSON_NULL_VAL = "null";

  public static final String CUSTOM_ORDERING = "converter.csv_to_json.custom_order";

  private List<String> customOrder;
  @Override
  public Converter<String, JsonArray, String[], JsonObject> init(WorkUnitState workUnit) {
    super.init(workUnit);
    customOrder = workUnit.getPropAsList(CUSTOM_ORDERING, "");
    if (!customOrder.isEmpty()) {
      LOG.info("Will use custom order to generate JSON from CSV: " + customOrder);
    }
    return this;
  }

  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    Preconditions.checkNotNull(inputSchema, "inputSchema is required.");
    return JSON_PARSER.parse(inputSchema).getAsJsonArray();
  }

  /**
   * Converts CSV (array of String) to JSON.
   * By default, fields between CSV and JSON are mapped in order bases and it validates if both input and output has same number of fields.
   *
   * Customization can be achieved by adding custom order where user can define list of indices of CSV fields correspond to output schema user defined.
   * Use case of customization (custom order):
   * In custom order, there are three input parameters generates output.
   *  1. Output schema: This is exact copy of input schema which is passed by user through job property.
   *  2. Custom order indices: This is indices passed by user through job property.
   *  3. Input record: This is CSV row, represented by array of String.
   * User usually does not have control on input record, and custom order is needed when output schema is not 1:1 match with input record.
   * Use cases:
   *  1. The order of input record(CSV in this case) does not match with output schema.
   *  2. Number of columns in output schema is greater or lesser than number of columns in input records.
   *
   * e.g:
   * 1. Different order
   * - Input record (CSV)
   *   "2029", "94043", "Mountain view"
   *
   * - Output schema (derived from input schema):
   * [{"columnName":"street_number","dataType":{"type":"string"}},{"columnName":"city","dataType":{"type":"string"}},{"columnName":"zip_code","dataType":{"type":"string"}}]
   *
   * - Custom order indices
   *   0,2,1
   *
   * - Output JSON (Key value is derived from output schema)
   *   {"street_number" : "2029", "city" : "Mountain view" , "zip_code" : "94043" }
   *
   * 2. # of columns in input record(CSV) > # of columns in output schema
   * - Input record (CSV)
   *   "2029", "Mountain view" , "USA", "94043"
   *
   * - Custom order indices
   *   0,1,3
   *
   * - Output schema (derived from input schema):
   * [{"columnName":"street_number","dataType":{"type":"string"}},{"columnName":"city","dataType":{"type":"string"}},{"columnName":"zip_code","dataType":{"type":"string"}}]
   *
   * - Output JSON (Key value is derived from output schema)
   *   {"street_number" : "2029", "city" : "Mountain view" , "zip_code" : "94043" }
   *
   * 3. # of columns in input record(CSV) < # of columns in output schema
   * - Input record (CSV)
   *   "2029", "Mountain view", "94043"
   *
   * - Custom order (adding null when negative index is defined)
   *   0,1,-1,2
   *
   * - Output schema (derived from input schema):
   * [{"columnName":"street_number","dataType":{"type":"string"}},{"columnName":"city","dataType":{"type":"string"}},
   *  {"columnName":"Country","isNullable":"true","dataType":{"type":"string"}},{"columnName":"zip_code","dataType":{"type":"string"}}]
   *
   * - Output JSON
   *   {"street_number" : "2029", "city" : "Mountain view" , "Country" : null, "zip_code" : "94043" }
   *
   * {@inheritDoc}
   * @see gobblin.converter.Converter#convertRecord(java.lang.Object, java.lang.Object, gobblin.configuration.WorkUnitState)
   */
  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, String[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    JsonObject outputRecord = null;
    if (!customOrder.isEmpty()) {
      outputRecord = createOutput(outputSchema, inputRecord, customOrder);
    } else {
      outputRecord = createOutput(outputSchema, inputRecord);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Converted into " + outputRecord);
    }
    return new SingleRecordIterable<JsonObject>(outputRecord);
  }

  @VisibleForTesting
  JsonObject createOutput(JsonArray outputSchema, String[] inputRecord) {
    Preconditions.checkArgument(outputSchema.size() == inputRecord.length, "# of columns mismatch. Input "
        + inputRecord.length + " , output: " + outputSchema.size());
    JsonObject outputRecord = new JsonObject();

    for (int i = 0; i < outputSchema.size(); i++) {
      String key = outputSchema.get(i).getAsJsonObject().get(COLUMN_NAME_KEY).getAsString();

      if (StringUtils.isEmpty(inputRecord[i]) || JSON_NULL_VAL.equalsIgnoreCase(inputRecord[i])) {
        outputRecord.add(key, JsonNull.INSTANCE);
      } else {
        outputRecord.addProperty(key, inputRecord[i]);
      }
    }

    return outputRecord;
  }

  @VisibleForTesting
  JsonObject createOutput(JsonArray outputSchema, String[] inputRecord, List<String> customOrder) {

    Preconditions.checkArgument(outputSchema.size() == customOrder.size(), "# of columns mismatch. Input "
        + outputSchema.size() + " , output: " + customOrder.size());
    JsonObject outputRecord = new JsonObject();
    Iterator<JsonElement> outputSchemaIterator = outputSchema.iterator();
    Iterator<String> customOrderIterator = customOrder.iterator();

    while(outputSchemaIterator.hasNext() && customOrderIterator.hasNext()) {
      String key = outputSchemaIterator.next().getAsJsonObject().get(COLUMN_NAME_KEY).getAsString();
      int i = Integer.parseInt(customOrderIterator.next());
      Preconditions.checkArgument(i < inputRecord.length, "Index out of bound detected in customer order. Index: " + i + " , # of CSV columns: " + inputRecord.length);
      if (i < 0 || null == inputRecord[i] || JSON_NULL_VAL.equalsIgnoreCase(inputRecord[i])) {
        outputRecord.add(key, JsonNull.INSTANCE);
        continue;
      }
      outputRecord.addProperty(key, inputRecord[i]);
    }

    return outputRecord;
  }
}
