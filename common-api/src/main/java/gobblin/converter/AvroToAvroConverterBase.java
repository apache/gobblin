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

package gobblin.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.WorkUnitState;


/**
 * A base abstract {@link Converter} class for data transformation from Avro to Avro.
 */
public abstract class AvroToAvroConverterBase extends Converter<Schema, Schema, GenericRecord, GenericRecord> {

  @Override
  public abstract Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException;

  @Override
  public abstract Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException;
}
