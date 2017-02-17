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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import gobblin.configuration.WorkUnitState;
import gobblin.type.RecordWithMetadata;
import gobblin.type.SerializedRecord;
import gobblin.type.SerializedRecordWithMetadata;


/**
 * Converts multiple source objects to a SerializedRecord object.
 */
public class AnyToSerializedRecordConverter extends Converter<String, String, Object, SerializedRecord> {

  private final static List<String> BINARY_CONTENT_TYPE = ImmutableList.of("application/octet-stream");
  private final static List<String> JSON_CONTENT_TYPE = ImmutableList.of("application/json");
  private final static Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<SerializedRecord> convertRecord(String outputSchema, Object inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<>(serializeRecord(inputRecord, null));
  }

  /**
   * Convert an arbitrary record into a serializedRecord.
   * eventually could pass a SerDe class in here as converter config and/or have SerDes register themselves
   */
  private SerializedRecord serializeRecord(Object inputRecord, List<String> knownContentType) {
    ByteBuffer serialized;
    List<String> defaultContentType;
    if (inputRecord instanceof SerializedRecord) {
      return (SerializedRecord) inputRecord;
    } else if (inputRecord instanceof byte[]) {
      serialized = ByteBuffer.wrap((byte[]) inputRecord);
      defaultContentType = BINARY_CONTENT_TYPE;
    } else if (inputRecord instanceof ByteBuffer) {
      serialized = (ByteBuffer) inputRecord;
      defaultContentType = BINARY_CONTENT_TYPE;
    } else if (inputRecord instanceof JsonElement) {
      JsonElement element = (JsonElement) inputRecord;
      serialized = ByteBuffer.wrap(GSON.toJson(element).getBytes(Charset.forName("UTF-8")));
      defaultContentType = JSON_CONTENT_TYPE;
    } else if (inputRecord instanceof RecordWithMetadata) {
      // Serializing a RecordWMetadata has two steps:
      // 1. We need to serialize the record inside of it which we can do recursively. This will convert
      //    the RecordWithMetadata into a SerializedRecordWithMetadata. If the metadata contains known
      //    content types we want to keep that type in the new record
      // 2. We need to serialize the {record + metadata} combination as JSON which will convert it back to a
      //    SerializedRecord
      RecordWithMetadata<?> inputRecordCasted = (RecordWithMetadata) inputRecord;
      String contentTypeFromRecord = (String) inputRecordCasted.getMetadata().get(RecordWithMetadata.RECORD_NAME);
      SerializedRecord innerRecord = serializeRecord(inputRecordCasted.getRecord(),
          contentTypeFromRecord == null ? null : ImmutableList.of(contentTypeFromRecord));
      SerializedRecordWithMetadata convertedRecord =
          new SerializedRecordWithMetadata(innerRecord, inputRecordCasted.getMetadata());

      return new SerializedRecord(ByteBuffer
          .wrap(SerializedRecord.getSerializedAwareGson().toJson(convertedRecord).getBytes(Charset.forName("UTF-8"))),
          SerializedRecordWithMetadata.CONTENT_TYPE_JSON);
    } else {
      throw new IllegalArgumentException(
          "Don't know how to serialize objects of type " + inputRecord.getClass().getCanonicalName());
    }

    return new SerializedRecord(serialized, knownContentType != null ? knownContentType : defaultContentType);
  }
}
