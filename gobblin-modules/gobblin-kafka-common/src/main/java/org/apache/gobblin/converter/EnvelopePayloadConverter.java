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

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;


/**
 * A converter decorates the envelope record with its payload deserialized into schema'ed object
 *
 * <p> Given an envelope schema as the input schema, the output schema will have the payload
 * field, configured by key {@value PAYLOAD_FIELD}, set with its latest schema fetched from a
 * {@link #registry} (see {@code createLatestPayloadField(Field)}). The converter copies the other fields
 * from the input schema to the output schema
 *
 * <p> Given an envelope record as the input record, the output record will have the payload set
 * to its deserialized object using the latest schema (see {@code convertPayload(GenericRecord)}).
 * The converter copies the other fields from the input record to the output record
 *
 * <p> If the current payload schema is incompatible with its latest schema, {@code convertPayload(GenericRecord)}
 * will throw an exception and the job fail
 */

public class EnvelopePayloadConverter extends BaseEnvelopeSchemaConverter<GenericRecord> {
  public static final String DECORATED_PAYLOAD_DOC = "Decorated payload data";

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    List<Field> outputSchemaFields = new ArrayList<>();
    for (Field field : inputSchema.getFields()) {
      outputSchemaFields.add(convertFieldSchema(inputSchema, field, workUnit));
    }

    Schema outputSchema = Schema
        .createRecord(inputSchema.getName(), inputSchema.getDoc(), inputSchema.getNamespace(), inputSchema.isError());
    outputSchema.setFields(outputSchemaFields);
    return outputSchema;
  }

  /**
   * Convert to the output schema of a field
   */
  protected Field convertFieldSchema(Schema inputSchema, Field field, WorkUnitState workUnit)
      throws SchemaConversionException {
    if (field.name().equals(payloadField)) {
      // Create a payload field with latest schema
      return createLatestPayloadField(field);
    }
    // Make a copy of the field to the output schema
    return new Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order());
  }

  /**
   * Create a payload field with its latest schema fetched from {@link #registry}
   *
   * @param field the original payload field from input envelope schema
   * @return a new payload field with its latest schema
   */
  private Field createLatestPayloadField(Field field)
      throws SchemaConversionException {
    try {
      Schema payloadSchema = fetchLatestPayloadSchema();
      return new Field(field.name(), payloadSchema, DECORATED_PAYLOAD_DOC, field.defaultValue(), field.order());
    } catch (Exception e) {
      throw new SchemaConversionException(e);
    }
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    GenericRecord outputRecord = new GenericData.Record(outputSchema);
    for (Field field : inputRecord.getSchema().getFields()) {
      outputRecord.put(field.name(), convertFieldValue(outputSchema, field, inputRecord, workUnit));
    }
    return new SingleRecordIterable<>(outputRecord);
  }

  /**
   * Convert to the output value of a field
   */
  protected Object convertFieldValue(Schema outputSchema, Field field, GenericRecord inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {
    if (field.name().equals(payloadField)) {
      return upConvertPayload(inputRecord);
    }
    return inputRecord.get(field.name());
  }
}
