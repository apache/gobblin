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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.filebased.TimestampAwareFileBasedHelper;
import org.apache.gobblin.source.extractor.hadoop.AvroFsHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used to load data from HDFS based on location and fields of selection,
 * and it returns the results as JsonArray.
 *
 * The reader will read sub-directories within the given location recursively, and pick up
 * all files in AVRO format by default.
 *
 * All files in the location should have consistent format and contain the fields of selection.
 *
 * @author vbhrill chrli
 */

public class HdfsReader {
  final private static String KEY_WORD_CATEGORY = "category";
  final private static String KEY_WORD_ACTIVATION = "activation";
  private JsonArray transientInputPayload;
  private static final Logger LOG = LoggerFactory.getLogger(HdfsReader.class);
  private State state;

  public HdfsReader(State state, JsonArray secondaryInputs) {
    this.transientInputPayload = secondaryInputs;
    this.state = state;
  }

  @VisibleForTesting
  public List<String> getFieldsAsList(JsonElement field) {
    List<String> fieldsList = new ArrayList<>();
    if (field.getAsJsonObject().has("fields")) {
      Iterator<JsonElement> iterator = field.getAsJsonObject()
          .get("fields").getAsJsonArray().iterator();
      while (iterator.hasNext()) {
        fieldsList.add(iterator.next().getAsString());
      }
    }
    return fieldsList;
  }

  /**
   * Reads secondary input paths one by one and return the JsonArrays by category
   * @return a Map<String, JsonArray> structure for records by category
   */
  public Map<String, JsonArray> readAll() {
    if (transientInputPayload == null || transientInputPayload.size() == 0) {
      return new HashMap<>();
    }
    Map<String, JsonArray> secondaryInput = new HashMap<>();
    for (JsonElement input: transientInputPayload) {
      JsonArray transientData = new JsonArray();
      JsonElement path = input.getAsJsonObject().get("path");
      List<String> fieldList = getFieldsAsList(input);
      String category = input.getAsJsonObject().has(KEY_WORD_CATEGORY)
          ? input.getAsJsonObject().get(KEY_WORD_CATEGORY).getAsString()
          : KEY_WORD_ACTIVATION;
      if (path != null) {
        transientData.addAll(readRecordsFromPath(path.getAsString(), fieldList, getFilters(input)));
        if (secondaryInput.containsKey(category)) {
          transientData.addAll(secondaryInput.get(category));
        }
        secondaryInput.put(category, transientData);
      }
    }
    return secondaryInput;
  }

  public JsonArray toJsonArray(String transientDataInputPayload) {
    try {
      return new Gson().fromJson(transientDataInputPayload, JsonArray.class);
    } catch (Exception e) {
      LOG.error("Error while processing transient input payload.");
      throw new RuntimeException("Error while processing transient input payload. Cannot convert into JsonArray.", e);
    }
  }

  private DataFileReader createDataReader(String path) {
    try {
      GenericDatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>();
      FsInput fsInput = new FsInput(new Path(path), new Configuration());
      return new DataFileReader(fsInput, genericDatumReader);
    } catch (Exception e) {
      throw new RuntimeException("Error initializing transient data reader", e);
    }
  }

  /**
   * process 1 secondary input path
   * @param inputLocation the secondary input path
   * @param fields the list of fields to be output
   * @param filters the list of filters to ber applied
   * @return a filter list of records
   */
  private JsonArray readRecordsFromPath(
      String inputLocation,
      List<String> fields,
      Map<String, String> filters) {
    JsonArray transientDataArray = new JsonArray();
    String sourceFileBasedFsUri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI);
    TimestampAwareFileBasedHelper fsHelper = new AvroFsHelper(state);
    try {
      state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, inputLocation);
      fsHelper.connect();
      List<String> filesToRead = fsHelper.ls(inputLocation);
      for (String singleFile: filesToRead) {
        DataFileReader reader = createDataReader(singleFile);
        transientDataArray.addAll(readFileAsJsonArray(reader, fields, filters));
      }
      return transientDataArray;
      } catch (Exception e) {
      throw new RuntimeException("Error while reading records from location " + inputLocation, e);
    } finally {
      if (sourceFileBasedFsUri != null) {
        state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, sourceFileBasedFsUri);
      }
    }
  }

  /**
   * This method read 1 avro file and store the records in a JsonArray
   *
   * @param preparedReader The avro file reader
   * @param fields The list of fields to output
   * @param filters The filters to apply to each records
   * @return the filtered projection of the avro file in a JsonArray
   */
  private JsonArray readFileAsJsonArray(
      DataFileReader preparedReader,
      List<String> fields,
      Map<String, String> filters) {
    JsonArray transientDataArray = new JsonArray();
    while (preparedReader.hasNext()) {
      GenericRecord record = (GenericRecord) preparedReader.next();
      Schema schema = record.getSchema();
      boolean recordAccepted = true;
      for (Schema.Field field: schema.getFields()) {
        String name = field.name();
        String pattern = filters.getOrDefault(name, ".*");
        if (record.get(name) != null && !record.get(name).toString().matches(pattern)
          || filters.keySet().contains(name) && record.get(name) == null) {
          recordAccepted = false;
        }
      }
      if (recordAccepted) {
        transientDataArray.add(selectFieldsFromGenericRecord(record, fields));
      }
    }
    return transientDataArray;
  }

  @VisibleForTesting
  private JsonObject selectFieldsFromGenericRecord(GenericRecord record, List<String> fields) {
    JsonObject jsonObject = new JsonObject();
    for (String field: fields) {
      Object valueObject = record.get(field);
      if (valueObject != null) {
        jsonObject.addProperty(field, EncryptionUtils.decryptGobblin(valueObject.toString(), state));
      } else {
        jsonObject.addProperty(field, (String) null);
      }
    }
    return jsonObject;
  }

  /**
   * retrieve the filters from the secondary input definition
   *
   * @param field a single secondary input source
   * @return the filters defined as a map of FieldName: RegEx
   */
  @VisibleForTesting
  private Map<String, String> getFilters(JsonElement field) {
    Map<String, String> filtersMap = new HashMap<>();
    if (field.getAsJsonObject().has("filters")) {
      JsonObject filterDefinition = field.getAsJsonObject().get("filters").getAsJsonObject();
      for (Map.Entry<String, JsonElement> entry : filterDefinition.entrySet()) {
        filtersMap.put(entry.getKey(), entry.getValue().getAsString());
      }
    }
    return filtersMap;
  }
}
