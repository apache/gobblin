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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.AvroToAvroConverterBase;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;


public class AvroFilterConverter extends AvroToAvroConverterBase {
  private static final Logger log = LoggerFactory.getLogger(AvroFilterConverter.class);
  private String[] fieldPath;
  private HashSet<String> filterIds;

  @Override
  public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnit) {
    if (!workUnit.contains(ConfigurationKeys.CONVERTER_FILTER_FIELD) || !workUnit
        .contains(ConfigurationKeys.CONVERTER_FILTER_IDS_FILE)) {
      throw new RuntimeException(
          "AvroFilterCoverter is not initialized properly please set: " + ConfigurationKeys.CONVERTER_FILTER_FIELD
              + " and " + ConfigurationKeys.CONVERTER_FILTER_IDS_FILE);
    }
    fieldPath = workUnit.getProp(ConfigurationKeys.CONVERTER_FILTER_FIELD).split("\\.");
    filterIds = new HashSet<String>();
    Path filterIdsLoc = new Path(workUnit.getProp(ConfigurationKeys.CONVERTER_FILTER_IDS_FILE));
    BufferedReader memberIdsReader = null;
    try {
      memberIdsReader = new BufferedReader(new FileReader(filterIdsLoc.getName()));
      String memberId;

      while ((memberId = memberIdsReader.readLine()) != null) {
        filterIds.add(memberId);
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    } finally {
      if (memberIdsReader != null) {
        try {
          memberIdsReader.close();
        } catch (IOException e) {
          // Do nothing
        }
      }
    }
    return super.init(workUnit);
  }

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public GenericRecord convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    if (filterIds.contains(extractField(inputRecord, fieldPath, 0))) {
      log.info("Dropping record: " + inputRecord);
      return null;
    } else {
      return inputRecord;
    }
  }

  /**
   * This method will only work with nested fields, it won't work for arrays or maps
   * @param data
   * @param fieldPath
   * @param field
   * @return
   */
  public String extractField(Object data, String[] fieldPath, int field) {
    if ((field + 1) == fieldPath.length) {
      String result = String.valueOf(((Record) data).get(fieldPath[field]));
      if (result == null) {
        return null;
      } else {
        return result;
      }
    } else {
      return extractField(((Record) data).get(fieldPath[field]), fieldPath, ++field);
    }
  }
}
