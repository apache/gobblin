/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter.filter;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.AvroToAvroConverterBase;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.util.AvroUtils;


/**
 * A {@link Converter} that removes certain fields from an Avro schema or an Avro record.
 *
 * @author ziliu
 */
public class AvroProjectionConverter extends AvroToAvroConverterBase {

  public static final String REMOVE_FIELDS = ".remove.fields";

  private Optional<AvroSchemaFieldRemover> fieldRemover;

  /**
   * To remove certain fields from the Avro schema or records of a topic/table, set property
   * {topic/table name}.remove.fields={comma-separated, fully qualified field names} in workUnit.
   *
   * E.g., PageViewEvent.remove.fields=header.memberId,mobileHeader.osVersion
   */
  @Override
  public AvroProjectionConverter init(WorkUnitState workUnit) {
    if (workUnit.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY)) {
      String removeFieldsPropName = workUnit.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY) + REMOVE_FIELDS;
      if (workUnit.contains(removeFieldsPropName)) {
        this.fieldRemover = Optional.of(new AvroSchemaFieldRemover(workUnit.getProp(removeFieldsPropName)));
      } else {
        this.fieldRemover = Optional.absent();
      }
    }
    return this;
  }

  /**
   * Remove the specified fields from inputSchema.
   */
  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    if (this.fieldRemover.isPresent()) {
      return this.fieldRemover.get().removeFields(inputSchema);
    } else {
      return inputSchema;
    }
  }

  /**
   * Convert the schema of inputRecord to outputSchema.
   */
  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      return new SingleRecordIterable<GenericRecord>(AvroUtils.convertRecordSchema(inputRecord, outputSchema));
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }

}
