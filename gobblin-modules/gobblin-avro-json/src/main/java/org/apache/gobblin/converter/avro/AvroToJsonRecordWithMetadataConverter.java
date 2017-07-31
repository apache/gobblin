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
package org.apache.gobblin.converter.avro;

import java.io.IOException;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.type.RecordWithMetadata;


public class AvroToJsonRecordWithMetadataConverter extends Converter<Schema, String, GenericRecord, RecordWithMetadata<JsonNode>> {
  private Metadata defaultMetadata;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private AvroToJsonStringConverterBase innerConverter;

  public AvroToJsonRecordWithMetadataConverter() {
    innerConverter = new AvroToJsonStringConverter();
  }

  @Override
  public String convertSchema(final Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {

    this.defaultMetadata = new Metadata();
    defaultMetadata.getGlobalMetadata().setContentType(inputSchema.getFullName() + "+json");

    return innerConverter.convertSchema(inputSchema, workUnit);
  }

  @Override
  public Iterable<RecordWithMetadata<JsonNode>> convertRecord(String outputSchema, GenericRecord inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {
    try {
      Iterable<String> innerRecordIterable = innerConverter.convertRecord(outputSchema, inputRecord, workUnit);
      String record = innerRecordIterable.iterator().next();

      JsonNode jsonRoot = objectMapper.readValue(record, JsonNode.class);
      return Collections.singleton(new RecordWithMetadata<JsonNode>(jsonRoot, defaultMetadata));
    } catch (IOException e) {
      throw new DataConversionException("Error converting to JSON", e);
    }
  }
}
