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

import org.apache.gobblin.configuration.WorkUnitState;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * A converter for extracting schema/records from an envelope schema.
 * Input schema: envelope schema - must have fields payloadSchemaId (the schema registry key of the output
 *               schema) and payload (byte data for output record)
 * Input record: record corresponding to input schema
 * Output schema: latest schema obtained from schema registry with topic {@link #PAYLOAD_SCHEMA_TOPIC}
 * Output record: record corresponding to output schema obtained from input record's {@link #PAYLOAD_FIELD} as bytes
 */
public class EnvelopePayloadExtractingConverter extends BaseEnvelopeSchemaConverter<GenericRecord> {
  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    try {
      return fetchLatestPayloadSchema();
    } catch (Exception e) {
      throw new SchemaConversionException(e);
    }
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<>(upConvertPayload(inputRecord));
  }
}
