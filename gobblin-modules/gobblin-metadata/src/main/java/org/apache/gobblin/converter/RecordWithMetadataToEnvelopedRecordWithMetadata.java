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

package org.apache.gobblin.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.types.GlobalMetadata;
import org.apache.gobblin.type.ContentTypeUtils;
import org.apache.gobblin.type.RecordWithMetadata;


/**
 * A converter that takes a {@link RecordWithMetadata} and serializes it using the following format:
 * {mId: "global metadata id", "rMd": recordMetadata, "r": record}
 *
 * The converter will also change the contentType in globalMetadata to lnkd+recordWithMetadata and record the
 * original contentType inside an inner-content-type header.
 *
 * The output of this converter is a valid UTF8-string encoded as a byte[].
 *
 * Note that this should be the last step in a converter chain - if global metadata is changed, its ID may change
 * as well which would lead to us embedding an incorrect metadata ID in the record.
 */
public class RecordWithMetadataToEnvelopedRecordWithMetadata extends Converter<Object, String, RecordWithMetadata<?>, RecordWithMetadata<byte[]>> {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final JsonFactory jsonFactory = new JsonFactory();
  private static final String CONTENT_TYPE = "lnkd+recordWithMetadata";

  @Override
  public String convertSchema(Object inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<RecordWithMetadata<byte[]>> convertRecord(String outputSchema, RecordWithMetadata<?> inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {

    try {
      updateRecordMetadata(inputRecord);

      ByteArrayOutputStream bOs = new ByteArrayOutputStream(512);
      try (JsonGenerator generator = jsonFactory.createJsonGenerator(bOs, JsonEncoding.UTF8).setCodec(objectMapper)) {
        generator.writeStartObject();

        writeHeaders(inputRecord, generator);
        writeRecord(inputRecord, generator);

        generator.writeEndObject();
      }

      return Collections.singleton(new RecordWithMetadata<byte[]>(bOs.toByteArray(), inputRecord.getMetadata()));
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }

  private void writeRecord(RecordWithMetadata<?> inputRecord, JsonGenerator generator)
      throws IOException {
    if (shouldInterpretRecordAsUtf8ByteArray(inputRecord)) {
      generator.writeFieldName("r");
      byte[] bytes = (byte[]) inputRecord.getRecord();
      generator.writeUTF8String(bytes, 0, bytes.length);
    } else {
      generator.writeObjectField("r", inputRecord.getRecord());
    }
  }

  private void writeHeaders(RecordWithMetadata<?> inputRecord, JsonGenerator generator)
      throws IOException {
    generator.writeStringField("mId", inputRecord.getMetadata().getGlobalMetadata().getId());

    if (!inputRecord.getMetadata().getRecordMetadata().isEmpty()) {
      generator.writeObjectField("rMd", inputRecord.getMetadata().getRecordMetadata());
    }
  }

  private void updateRecordMetadata(RecordWithMetadata<?> inputRecord) {
    GlobalMetadata md = inputRecord.getMetadata().getGlobalMetadata();

    String origContentType = md.getContentType();
    if (origContentType != null) {
      md.setInnerContentType(origContentType);
    }

    md.setContentType(CONTENT_TYPE);
    md.markImmutable();
  }

  private boolean shouldInterpretRecordAsUtf8ByteArray(RecordWithMetadata<?> inputRecord) {
    if (inputRecord.getRecord() instanceof byte[]) {
      return ContentTypeUtils.getInstance().inferPrintableFromMetadata(inputRecord.getMetadata());
    }

    return false;
  }
}
