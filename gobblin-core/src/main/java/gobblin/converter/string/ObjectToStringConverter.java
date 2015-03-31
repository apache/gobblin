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

package gobblin.converter.string;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

/**
 * Implementation of {@link Converter} that converts a given {@link Object} to its {@link String} representation
 */
public class ObjectToStringConverter extends Converter<Object, Class<String>, Object, String> {

  @Override
  public Class<String> convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return String.class;
  }

  @Override
  public Iterable<String> convertRecord(Class<String> outputSchema, Object inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<String>(inputRecord.toString());
  }
}
