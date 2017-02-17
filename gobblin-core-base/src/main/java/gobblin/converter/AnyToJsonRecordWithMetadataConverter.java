/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.converter;

import java.util.Collections;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import gobblin.configuration.WorkUnitState;
import gobblin.type.RecordWithMetadata;


/**
 * A converter that converts any type to a {@link RecordWithMetadata}.
 * Ensures that the contained Record is a JsonElement. If the recordType can be retrieved from the
 * object or inferred from the input schema, it will be placed in metadata.
 * Caveat: Currently only supports Avro and Json input
 */
public class AnyToJsonRecordWithMetadataConverter extends Converter<Object, String, Object, RecordWithMetadata<JsonElement>> {
  private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
  private String recordTypeName;

  @Override
  public String convertSchema(Object inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    // Store the recordName if we know what it is
    if (recordTypeName == null && inputSchema != null && inputSchema instanceof Schema) {
      recordTypeName = ((Schema) inputSchema).getFullName();
    }
    return "";
  }

  @Override
  public Iterable<RecordWithMetadata<JsonElement>> convertRecord(String outputSchema, Object inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {
    if (inputRecord instanceof RecordWithMetadata && ((RecordWithMetadata) inputRecord)
        .getRecord() instanceof JsonElement) {
      // pass through
      @SuppressWarnings("unchecked")
      RecordWithMetadata<JsonElement> jsonRecord = (RecordWithMetadata) inputRecord;

      return new SingleRecordIterable<>(jsonRecord);
    } else if (inputRecord instanceof JsonElement) {
      JsonElement jsonElement = (JsonElement) inputRecord;
      Map<String, Object> metadata;

      if (recordTypeName != null) {
        metadata = ImmutableMap.<String, Object>of(RecordWithMetadata.RECORD_NAME, recordTypeName);
      } else {
        metadata = Collections.emptyMap();
      }

      return new SingleRecordIterable<>(new RecordWithMetadata<>(jsonElement, metadata));
    } else if (inputRecord instanceof GenericRecord) {
      GenericRecord genericRecord = (GenericRecord) inputRecord;
      Map<String, Object> metadata =
          ImmutableMap.<String, Object>of(RecordWithMetadata.RECORD_NAME, genericRecord.getSchema().getFullName() + "+json");
      String json = GenericData.get().toString(genericRecord);
      JsonElement jsonElement = GSON.fromJson(json, JsonElement.class);
      return new SingleRecordIterable<>(new RecordWithMetadata<>(jsonElement, metadata));
    } else {
      throw new DataConversionException("Cannot convert records of type " + inputRecord);
    }
  }
}
