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

package com.linkedin.uif.converter.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.util.AvroUtils;

/**
 * A converter class where the input is an Avro record, and the output is a specific field in that record. Since the
 * field can be of any type this Converter returns a Java {@link Object}. The parameter converter.avro.extractor.field.path
 * specifies the location of the field to retrieve. Nested fields can be specified by following use the following
 * syntax: field.nestedField
 */
public class AvroFieldRetrieverConverter extends Converter<Schema, Class<Object>, GenericRecord, Object> {

  private String fieldLocation;

  @Override
  public Converter<Schema, Class<Object>, GenericRecord, Object> init(WorkUnitState workUnit) {
    Preconditions.checkArgument(workUnit.contains(ConfigurationKeys.CONVERTER_AVRO_EXTRACTOR_FIELD_PATH),
        "Missing required property converter.avro.extractor.field.path for the AvroFieldRetrieverConverter class.");
    this.fieldLocation = workUnit.getProp(ConfigurationKeys.CONVERTER_AVRO_EXTRACTOR_FIELD_PATH);
    return this;
  }

  @Override
  public Class<Object> convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return Object.class;
  }

  @Override
  public Object convertRecord(Class<Object> outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    Optional<Object> logField = AvroUtils.getField(inputRecord, this.fieldLocation);
    if (logField.isPresent()) {
      return logField.get();
    } else {
      return null;
    }
  }
}
