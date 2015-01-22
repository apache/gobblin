/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.converter.filter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.AvroToAvroConverterBase;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.util.AvroUtils;

/**
 * Basic implementation of a filter converter for Avro data. It filters out Avro records based on a specified Avro
 * field name, and its expected value. The converter only supports equality operations and only performs the comparison
 * based on the string representation of the value.
 */
public class AvroFilterConverter extends AvroToAvroConverterBase {

  private String fieldName;
  private String fieldValue;

  /**
   * The config must specify {@link ConfigurationKeys.CONVERTER_FILTER_FIELD_NAME} to indicate which field to retrieve
   * from the Avro record and {@link ConfigurationKeys.CONVERTER_FILTER_FIELD_VALUE} to indicate the expected value of
   * the field.
   * {@inheritDoc}
   * @see com.linkedin.uif.converter.Converter#init(com.linkedin.uif.configuration.WorkUnitState)
   */
  @Override
  public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnit) {
    Preconditions.checkArgument(workUnit.contains(ConfigurationKeys.CONVERTER_FILTER_FIELD_NAME),
        "Missing required property converter.filter.field for the AvroFilterConverter class.");
    Preconditions.checkArgument(workUnit.contains(ConfigurationKeys.CONVERTER_FILTER_FIELD_VALUE),
        "Missing required property converter.filter.value for the AvroFilterConverter class.");

    this.fieldName = workUnit.getProp(ConfigurationKeys.CONVERTER_FILTER_FIELD_NAME);
    this.fieldValue = workUnit.getProp(ConfigurationKeys.CONVERTER_FILTER_FIELD_VALUE);
    return super.init(workUnit);
  }

  /**
   * Returns the inputSchema unmodified.
   * {@inheritDoc}
   * @see com.linkedin.uif.converter.AvroToAvroConverterBase#convertSchema(org.apache.avro.Schema, com.linkedin.uif.configuration.WorkUnitState)
   */
  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  /**
   * Retrieves the specified field from the inputRecord, and checks if it is equal to the expected value
   * {@link #fieldValue}. If it is then it returns the inputRecord, if not it returns null.
   * {@inheritDoc}
   * @see com.linkedin.uif.converter.AvroToAvroConverterBase#convertRecord(org.apache.avro.Schema, org.apache.avro.generic.GenericRecord, com.linkedin.uif.configuration.WorkUnitState)
   */
  @Override
  public GenericRecord convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    Optional<Object> fieldValue = AvroUtils.getFieldValue(inputRecord, this.fieldName);
    if (fieldValue.isPresent() && fieldValue.get().toString().equals(this.fieldValue)) {
      return inputRecord;
    }
    return null;
  }
}
