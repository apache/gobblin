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
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.EmptyIterable;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ForkOperatorUtils;


/**
 * A converter class where the input is an Avro record, and the output is a specific field in that record. Since the
 * field can be of any type this Converter returns a Java {@link Object}. The parameter converter.avro.extractor.field.path
 * specifies the location of the field to retrieve. Nested fields can be specified by following use the following
 * syntax: field.nestedField
 */
public class AvroFieldRetrieverConverter extends Converter<Schema, Schema, GenericRecord, Object> {

  private String fieldLocation;

  @Override
  public Converter<Schema, Schema, GenericRecord, Object> init(WorkUnitState workUnit) {

    String fieldPathKey =
        ForkOperatorUtils.getPropertyNameForBranch(workUnit,
            ConfigurationKeys.CONVERTER_AVRO_EXTRACTOR_FIELD_PATH);

    Preconditions.checkArgument(workUnit.contains(fieldPathKey),
        "The converter " + this.getClass().getName() + " cannot be used without setting the property "
            + ConfigurationKeys.CONVERTER_AVRO_EXTRACTOR_FIELD_PATH);

    this.fieldLocation = workUnit.getProp(fieldPathKey);
    return this;
  }

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    Optional<Schema> schema = AvroUtils.getFieldSchema(inputSchema, this.fieldLocation);

    return schema.orNull();
  }

  @Override
  public Iterable<Object> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    Optional<Object> field = AvroUtils.getFieldValue(inputRecord, this.fieldLocation);

    return field.isPresent() ? new SingleRecordIterable<>(field.get()) : new EmptyIterable<>();
  }
}
