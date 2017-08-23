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
import com.google.gson.JsonObject;


/**
 * {@link Converter} that takes an Avro schema in string format and corresponding {@link JsonObject} records and
 * converts them to {@link GenericRecord} using the schema
 */
public class JsonRecordAvroSchemaToAvroConverter extends ToAvroConverterBase<String, JsonObject> {

  /**
   * Take an Avro schema in string format and parse it into a {@link Schema}
   */
  @Override
  public Schema convertSchema(String schema, WorkUnitState workUnit) throws SchemaConversionException {
    return new Schema.Parser().parse(schema);
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
      Schema.Type type = field.schema().getType();
      if (type.equals(Schema.Type.RECORD)) {
        avroRecord.put(field.name(), convertNestedRecord(field.schema(), inputRecord.get(field.name()).getAsJsonObject(), workUnit));
      } else {
        boolean nullable = false;

        if (type.equals(Schema.Type.UNION)) {
          nullable = true;
          List<Schema> types = field.schema().getTypes();
          if (types.size() != 2) {
            throw new DataConversionException("Only unions of size 2 supported");
          }
          if (field.schema().getTypes().get(0).getType().equals(Schema.Type.NULL)) {
            type = field.schema().getTypes().get(1).getType();
          } else if (field.schema().getTypes().get(1).getType().equals(Schema.Type.NULL)) {
            type = field.schema().getTypes().get(0).getType();
          } else {
            throw new DataConversionException("Union must contain null");
          }
        }
        try {
          converter = JsonElementConversionWithAvroSchemaFactory.getConvertor(field.name(), type.getName(), field.schema(),
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
