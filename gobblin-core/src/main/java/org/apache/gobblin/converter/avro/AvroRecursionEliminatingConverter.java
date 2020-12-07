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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.util.Pair;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.AvroToAvroConverterBase;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.AvroUtils;


/**
 * A converter that removes recursion from Avro Generic Records
 */
@Slf4j
public class AvroRecursionEliminatingConverter extends AvroToAvroConverterBase {

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    Pair<Schema, List<AvroUtils.SchemaEntry>> results = AvroUtils.dropRecursiveFields(inputSchema);
    List<AvroUtils.SchemaEntry> recursiveFields = results.getSecond();
    if (!recursiveFields.isEmpty()) {
      log.warn("Schema {} is recursive. Will drop fields [{}]", inputSchema.getFullName(),
          recursiveFields.stream().map(entry -> entry.getFieldName()).collect(Collectors.joining(",")));
      log.debug("Projected Schema = {}", results.getFirst());
    }
    return results.getFirst();
  }

  @Override
  public Iterable<GenericRecord> convertRecordImpl(Schema outputSchema, GenericRecord inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {
    try {
      return new SingleRecordIterable(AvroUtils.convertRecordSchema(inputRecord, outputSchema));
    } catch (IOException e) {
      throw new DataConversionException("Failed to convert", e);
    }
  }
}
