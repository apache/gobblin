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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import javassist.bytecode.ByteArray;

import gobblin.configuration.WorkUnitState;
import gobblin.metadata.types.Metadata;
import gobblin.type.ContentTypeUtils;
import gobblin.type.RecordWithMetadata;


/**
 * A converter that converts any type to a {@link RecordWithMetadata}.
 * Ensures that the contained Record is a JsonElement. If the recordType can be retrieved from the
 * object or inferred from the input schema, it will be placed in metadata.
 * Caveat: Currently only supports Avro and Json input
 */
public class RecordWithMetadataToEnvelopedRecordWithMetadata extends Converter<Object, String, RecordWithMetadata<?>,
    RecordWithMetadata<byte[]>> {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final JsonFactory jsonFactory = new JsonFactory();
  private static final String CONTENT_TYPE = "lnkd+recordWithMetadata";

  @Override
  public String convertSchema(Object inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    // Store the recordName if we know what it is
    return "";
  }

  @Override
  public Iterable<RecordWithMetadata<byte[]>> convertRecord(String outputSchema, RecordWithMetadata<?> inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {

    try {
      ByteArrayOutputStream bOs = new ByteArrayOutputStream(512);
      try (JsonGenerator generator = jsonFactory.createJsonGenerator(bOs, JsonEncoding.UTF8).setCodec(objectMapper)) {
        generator.writeStartObject();
        generator.writeStringField("mId", inputRecord.getMetadata().getGlobalMetadata().getId());

        if (!inputRecord.getMetadata().getRecordMetadata().isEmpty()) {
          generator.writeObjectField("rMd", inputRecord.getMetadata().getRecordMetadata());
        }

        if (treatRecordAsUtf8String(inputRecord)) {
          generator.writeFieldName("r");
          byte[] bytes = (byte[])inputRecord.getRecord();
          generator.writeUTF8String(bytes, 0, bytes.length);
        } else {
          generator.writeObjectField("r", inputRecord.getRecord());
        }

        generator.writeEndObject();
      }

      Metadata md = inputRecord.getMetadata();
      String origContentType = md.getGlobalMetadata().getContentType();
      if (origContentType != null) {
        md.getGlobalMetadata().setInnerContentType(origContentType);
      }
      md.getGlobalMetadata().setContentType(CONTENT_TYPE);
      return Collections.singleton(new RecordWithMetadata<byte[]>(bOs.toByteArray(), md));
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }

  private boolean treatRecordAsUtf8String(RecordWithMetadata<?> inputRecord) {
    if (inputRecord.getRecord() instanceof byte[]) {
      return ContentTypeUtils.getInstance().inferPrintableFromMetadata(inputRecord.getMetadata());
    }

    return false;
  }
}
