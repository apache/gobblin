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

package gobblin.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.WorkUnitState;


/**
 * A base abstract {@link Converter} class for data transformation to Avro.
 *
 * @param <SI> input schema type
 * @param <DI> input data type
 */
public abstract class ToAvroConverterBase<SI, DI> extends Converter<SI, Schema, DI, GenericRecord> {

  @Override
  public abstract Schema convertSchema(SI schema, WorkUnitState workUnit)
      throws SchemaConversionException;

  @Override
  public abstract Iterable<GenericRecord> convertRecord(Schema outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException;
}
