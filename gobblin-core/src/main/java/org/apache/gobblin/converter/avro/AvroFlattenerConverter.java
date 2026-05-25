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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.AvroFlattener;
import org.apache.gobblin.util.AvroUtils;


/**
 * Flatten an avro record with {@link AvroFlattener}
 *
 * <p>
 *   A {@link AvroFlattenerConverter} will flatten avro. It converts
 *   {@link Field} with name <code>"address.city"</code> to field name <code>address__city</code>
 * </p>
 */
public class AvroFlattenerConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {
  private static AvroFlattener AVRO_FLATTENER = new AvroFlattener();

  private static final Logger LOG = Logger.getLogger(AvroFlattenerConverter.class);

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return AVRO_FLATTENER.flatten(inputSchema, true);
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    GenericRecord outputRecord = new GenericData.Record(outputSchema);
    for (Field field : outputSchema.getFields()) {

      String flattenSource = field.getProp("flatten_source");
      if (StringUtils.isBlank(flattenSource)) {
        flattenSource = field.name();
      }

      outputRecord.put(field.name(), AvroUtils.getFieldValue(inputRecord, flattenSource).orNull());
    }

    return new SingleRecordIterable<>(outputRecord);
  }
}
