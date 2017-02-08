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

import java.util.HashMap;

import org.apache.avro.generic.GenericRecord;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import gobblin.configuration.WorkUnitState;
import gobblin.type.RecordWithMetadata;


/**
 * A converter that converts any type to a {@link RecordWithMetadata}.
 * Ensures that the contained Record is a JsonElement
 * Caveat: Currently only supports Avro and Json input
 */
public class AnyToRecordWithMetadataConverter extends Converter<Object, String, Object, RecordWithMetadata> {
  private static final Gson GSON = new Gson();
  @Override
  public String convertSchema(Object inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<RecordWithMetadata> convertRecord(String outputSchema, Object inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    if (inputRecord instanceof RecordWithMetadata) {
      // pass through
      return new SingleRecordIterable<>((RecordWithMetadata) inputRecord);
    }
    if (inputRecord instanceof JsonElement) {
      return new SingleRecordIterable<>(new RecordWithMetadata(inputRecord, new HashMap<String, Object>()));
    }
    else if (inputRecord instanceof GenericRecord) {
      // convert to Json
      JsonElement jsonElement = GSON.toJsonTree(inputRecord);
      return new SingleRecordIterable<>(new RecordWithMetadata(jsonElement, new HashMap<String, Object>()));
    } else {
      throw new DataConversionException("Cannot convert records of type " + inputRecord);
    }
  }
}
