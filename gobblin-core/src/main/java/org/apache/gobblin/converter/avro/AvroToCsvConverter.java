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

import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;

import com.google.common.collect.Lists;


/**
 * Convert an Avro GenericRecord to its CSV representation. Note: This converter only converts
 * top level record fields to CSV and ignores the rest. It does not performs any flattening
 * of data.
 */
public class AvroToCsvConverter extends Converter<Schema, String, GenericRecord, String> {
  private static final String DEFAULT_COLUMN_DELIMITER = ",";

  private String columnDelimiter;
  private boolean isHeaderIncluded;
  private List<String> columnNames;

  @Override
  public String convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    if (null == inputSchema) {
      throw new IllegalArgumentException("Null Avro record schema is not allowed.");
    }
    if (!inputSchema.getType().equals(Schema.Type.RECORD)) {
      throw new IllegalArgumentException(String.format("Schema for table must be of type RECORD. Received type: %s",
          inputSchema.getType()));
    }

    columnNames = Lists.newArrayListWithExpectedSize(inputSchema.getFields().size());
    columnDelimiter = workUnit.getProp(ConfigurationKeys.CONVERTER_AVRO_TO_CSV_DELIMITER, DEFAULT_COLUMN_DELIMITER);
    isHeaderIncluded = workUnit.getPropAsBoolean(ConfigurationKeys.CONVERTER_AVRO_TO_CSV_IS_HEADER_INCLUDED, false);
    for (Schema.Field columnField : inputSchema.getFields()) {
      columnNames.add(columnField.name());
    }

    return StringUtils.join(columnNames, columnDelimiter);
  }

  @Override
  public Iterable<String> convertRecord(String outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    if (null == inputRecord) {
      throw new IllegalArgumentException("Null Avro record is not allowed.");
    }
    try {
      List<String> columnValues = Lists.newArrayListWithExpectedSize(columnNames.size());
      for (String columnName : columnNames) {
        // This is assumed to be String or Optional (Union of NULL, String)
        String value = inputRecord.get(columnName) == null ? null : inputRecord.get(columnName).toString();
        columnValues.add(value);
      }

      if (isHeaderIncluded) {
        isHeaderIncluded = false; // Turn off for subsequent records
        return Lists.newArrayList(StringUtils.join(columnNames, columnDelimiter),
            StringUtils.join(columnValues, columnDelimiter));
      } else {
        return Collections.singleton(StringUtils.join(columnValues, columnDelimiter));
      }

    } catch (Exception e) {
      throw new DataConversionException("Error serializing record", e);
    }
  }
}
