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
import gobblin.type.SerializedRecordWithMetadata;


/**
 * A converter that converts a {@link RecordWithMetadata} to a {@link gobblin.type.SerializedRecordWithMetadata}
 * where the serialized bytes represent encrypted data.
 */
public class EncryptedRecordWithMetadataConverter extends Converter<String, String, RecordWithMetadata, SerializedRecordWithMetadata> {
  private static final Gson GSON = new Gson();

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<SerializedRecordWithMetadata> convertRecord(String outputSchema, RecordWithMetadata inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {

    //TODO: Encrypt this
    byte[] serialized = GSON.toJson(inputRecord.getRecord()).getBytes();
    SerializedRecordWithMetadata serializedRecordWithMetadata = new SerializedRecordWithMetadata(serialized,
        inputRecord.getMetadata());
    return new SingleRecordIterable<>(serializedRecordWithMetadata);
  }
}
