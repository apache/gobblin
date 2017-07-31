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

import java.io.IOException;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.type.RecordWithMetadata;


/**
 * Wraps a given converter to accept a RecordWithMetadata with a given data type, convert the underlying record using
 * the wrapped converter, and pass the metadata through untouched
 */
public class MetadataConverterWrapper<SI, SO, DI, DO> extends Converter<SI, SO, Object, RecordWithMetadata<DO>> {
  private final Converter<SI, SO, DI, DO> innerConverter;

  public MetadataConverterWrapper(Converter<SI, SO, DI, DO> innerConverter) {
    this.innerConverter = innerConverter;
  }

  @Override
  public Converter<SI, SO, Object, RecordWithMetadata<DO>> init(WorkUnitState workUnit) {
    super.init(workUnit);
    innerConverter.init(workUnit);

    return this;
  }

  @Override
  public void close()
      throws IOException {
    innerConverter.close();
  }

  @Override
  public SO convertSchema(SI inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return innerConverter.convertSchema(inputSchema, workUnit);
  }

  @Override
  public Iterable<RecordWithMetadata<DO>> convertRecord(SO outputSchema, Object inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    final Metadata metadata = getMetadataFromRecord(inputRecord);
    final DI innerRecord = getRecordFromRecord(inputRecord);

    Iterable<DO> outputRecords = innerConverter.convertRecord(outputSchema, innerRecord, workUnit);

    return Iterables.transform(outputRecords, new Function<DO, RecordWithMetadata<DO>>() {
      @Nullable
      @Override
      public RecordWithMetadata<DO> apply(@Nullable DO input) {
        return new RecordWithMetadata<>(input, metadata);
      }
    });
  }

  @SuppressWarnings("unchecked")
  private DI getRecordFromRecord(Object inputRecord) {
    if (inputRecord instanceof RecordWithMetadata<?>) {
      Object uncastedRecord = ((RecordWithMetadata) inputRecord).getRecord();
      return (DI)uncastedRecord;
    } else {
      return (DI)inputRecord;
    }
  }

  private Metadata getMetadataFromRecord(Object inputRecord) {
    if (inputRecord instanceof RecordWithMetadata<?>) {
      return ((RecordWithMetadata) inputRecord).getMetadata();
    } else {
      return new Metadata();
    }
  }

  @Override
  public State getFinalState() {
    return innerConverter.getFinalState();
  }

  /**
   * Intended to be overridden by converters who want to manipulate metadata as it flows through
   * @param metadata
   * @return
   */
  protected Metadata convertMetadata(Metadata metadata) {
    return metadata;
  }
}
