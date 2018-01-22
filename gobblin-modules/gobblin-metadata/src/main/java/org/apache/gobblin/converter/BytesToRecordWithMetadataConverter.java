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
package org.apache.gobblin.converter;

import java.util.Collections;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.type.RecordWithMetadata;


/**
 * A converter that takes an array of bytes and convert it to {@link RecordWithMetadata}
 * where record will be array of bytes and Metadata will be empty initialization
 */
public class BytesToRecordWithMetadataConverter extends Converter<Object, Object, byte[], RecordWithMetadata<?>> {
  @Override
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<RecordWithMetadata<?>> convertRecord(Object outputSchema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return Collections.singleton(new RecordWithMetadata<>(inputRecord, new Metadata()));
  }
}
