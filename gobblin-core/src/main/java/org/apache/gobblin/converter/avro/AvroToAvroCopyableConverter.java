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

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.fork.CopyableGenericRecord;
import org.apache.gobblin.fork.CopyableSchema;


/**
 * Implementation of {@link Converter} that takes in an Avro {@link Schema} and {@link GenericRecord} and returns a
 * {@link org.apache.gobblin.fork.CopyableSchema} and a {@link org.apache.gobblin.fork.CopyableGenericRecord}.
 */
public class AvroToAvroCopyableConverter extends
    Converter<Schema, CopyableSchema, GenericRecord, CopyableGenericRecord> {

  /**
   * Returns a {@link org.apache.gobblin.fork.CopyableSchema} wrapper around the given {@link Schema}.
   * {@inheritDoc}
   * @see org.apache.gobblin.converter.Converter#convertSchema(java.lang.Object, org.apache.gobblin.configuration.WorkUnitState)
   */
  @Override
  public CopyableSchema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return new CopyableSchema(inputSchema);
  }

  /**
   * Returns a {@link org.apache.gobblin.fork.CopyableGenericRecord} wrapper around the given {@link GenericRecord}.
   * {@inheritDoc}
   * @see org.apache.gobblin.converter.Converter#convertRecord(java.lang.Object, java.lang.Object, org.apache.gobblin.configuration.WorkUnitState)
   */
  @Override
  public Iterable<CopyableGenericRecord> convertRecord(CopyableSchema outputSchema, GenericRecord inputRecord,
      WorkUnitState workUnit) throws DataConversionException {
    return new SingleRecordIterable<>(new CopyableGenericRecord(inputRecord));
  }
}
