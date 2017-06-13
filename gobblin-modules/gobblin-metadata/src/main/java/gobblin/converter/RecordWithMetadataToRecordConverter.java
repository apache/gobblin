/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package gobblin.converter;

import java.util.Collections;

import gobblin.configuration.WorkUnitState;
import gobblin.type.RecordWithMetadata;


/**
 * Convert a RecordWithMetadata to its underlying Record, discarding any metadata.
 */
public class RecordWithMetadataToRecordConverter extends Converter<Object, Object, RecordWithMetadata, Object> {
  @Override
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<Object> convertRecord(Object outputSchema, RecordWithMetadata inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return Collections.singleton(inputRecord.getRecord());
  }
}
