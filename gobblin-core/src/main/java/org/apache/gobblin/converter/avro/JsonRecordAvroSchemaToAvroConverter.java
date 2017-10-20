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

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.converter.ToAvroConverterBase;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import com.google.gson.JsonNull;
import com.google.common.base.Splitter;


/**
 * {@link Converter} that takes an Avro schema from config and corresponding {@link JsonObject} records and
 * converts them to {@link GenericRecord} using the schema
 */
public class JsonRecordAvroSchemaToAvroConverter<SI> extends ToAvroConverterBase<SI, JsonObject> {

  private static final Splitter SPLITTER_ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();

  public static final String AVRO_SCHEMA_KEY = "converter.avroSchema";
  public static final String IGNORE_FIELDS = "converter.ignoreFields";

  private Schema schema;
  private List<String> ignoreFields;

  public ToAvroConverterBase<SI, JsonObject> init(WorkUnitState workUnit) {
    super.init(workUnit);
    Preconditions.checkArgument(workUnit.contains(AVRO_SCHEMA_KEY));
    this.schema = new Schema.Parser().parse(workUnit.getProp(AVRO_SCHEMA_KEY));
    this.ignoreFields = SPLITTER_ON_COMMA.splitToList(workUnit.getProp(IGNORE_FIELDS, ""));
    return this;
  }

  /**
   * Ignore input schema and parse in Avro schema from config
   */
  @Override
  public Schema convertSchema(SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return this.schema;
  }

  /**
   * Take in {@link JsonObject} input records and convert them to {@link GenericRecord} using outputSchema
   */
  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    GenericRecord avroRecord = convertNestedRecord(outputSchema, inputRecord, workUnit);
    return new SingleRecordIterable<>(avroRecord);
  }

  private GenericRecord convertNestedRecord(Schema outputSchema, JsonObject inputRecord, WorkUnitState workUnit) throws DataConversionException {
    GenericRecord avroRecord = new GenericData.Record(outputSchema);
    JsonElementConversionWithAvroSchemaFactory.JsonElementConverter converter;
    for (Schema.Field field : outputSchema.getFields()) {

      if (this.ignoreFields.contains(field.name())) {
        continue;
      }

      Schema.Type type = field.schema().getType();
      boolean nullable = false;
      Schema schema = field.schema();

      if (type.equals(Schema.Type.UNION)) {
        nullable = true;
        List<Schema> types = field.schema().getTypes();
        if (types.size() != 2) {
          throw new DataConversionException("Unions must be size 2, and contain one null");
        }
        if (field.schema().getTypes().get(0).getType().equals(Schema.Type.NULL)) {
          schema = field.schema().getTypes().get(1);
          type = schema.getType();
        } else if (field.schema().getTypes().get(1).getType().equals(Schema.Type.NULL)) {
          schema = field.schema().getTypes().get(0);
          type = schema.getType();
        } else {
          throw new DataConversionException("Unions must be size 2, and contain one null");
        }

        if (inputRecord.get(field.name()) == null) {
          inputRecord.add(field.name(), JsonNull.INSTANCE);
        }
      }

      if (inputRecord.get(field.name()) == null) {
        throw new DataConversionException("Field missing from record: " + field.name());
      }

      if (type.equals(Schema.Type.RECORD)) {
        if (nullable && inputRecord.get(field.name()).isJsonNull()) {
          avroRecord.put(field.name(), null);
        } else {
          avroRecord.put(field.name(),
              convertNestedRecord(schema, inputRecord.get(field.name()).getAsJsonObject(), workUnit));
        }
      } else {
        try {
          converter = JsonElementConversionWithAvroSchemaFactory.getConvertor(field.name(), type.getName(), schema,
              workUnit, nullable);
          avroRecord.put(field.name(), converter.convert(inputRecord.get(field.name())));
        } catch (Exception e) {
          throw new DataConversionException("Could not convert field " + field.name());
        }
      }
    }
    return avroRecord;
  }
}
