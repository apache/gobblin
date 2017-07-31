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

package org.apache.gobblin.converter.filter;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.AvroToAvroConverterBase;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.AvroUtils;


/**
 * A {@link Converter} that removes certain fields from an Avro schema or an Avro record.
 *
 * @author Ziyang Liu
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
    }
    return inputSchema;
  }

  /**
   * Convert the schema of inputRecord to outputSchema.
   */
  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      return new SingleRecordIterable<>(AvroUtils.convertRecordSchema(inputRecord, outputSchema));
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }

}
