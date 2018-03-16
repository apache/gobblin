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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.type.RecordWithMetadata;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * A converter that takes a {@link RecordWithMetadata} and deserializes it by trying to parse it into a
 * json format. It looks up two fields: "rMd" for record metadata and "r" for record details represented
 * as a string.
 */
public class EnvelopedRecordWithMetadataToRecordWithMetadata extends Converter<String, Object, RecordWithMetadata<byte[]>, RecordWithMetadata<?>>  {

  private static final String RECORD_KEY = "r";
  private static final String METADATA_KEY = "rMd";
  private static final String METADATA_RECORD_KEY = "recordMetadata";

  private static final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final JsonFactory jsonFactory = new JsonFactory();

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<RecordWithMetadata<?>> convertRecord(Object outputSchema, RecordWithMetadata<byte[]> inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {

    try {
      try (JsonParser parser = jsonFactory.createJsonParser(inputRecord.getRecord())) {
        parser.setCodec(objectMapper);
        JsonNode jsonNode = parser.readValueAsTree();

        // extracts required record
        if (!jsonNode.has(RECORD_KEY)) {
          throw new DataConversionException("Input data does not have record.");
        }
        String record = jsonNode.get(RECORD_KEY).getTextValue();

        // Extract metadata field
        Metadata md = new Metadata();
        if (jsonNode.has(METADATA_KEY) && jsonNode.get(METADATA_KEY).has(METADATA_RECORD_KEY)) {
          md.getRecordMetadata().putAll(objectMapper.readValue(jsonNode.get(METADATA_KEY).get(METADATA_RECORD_KEY), Map.class));
        }

        return Collections.singleton(new RecordWithMetadata<>(record, md));
      }
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }

}
