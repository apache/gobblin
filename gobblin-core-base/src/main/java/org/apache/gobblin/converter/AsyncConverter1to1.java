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

import java.util.concurrent.CompletableFuture;

import gobblin.annotation.Alpha;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.stream.ControlMessage;
import gobblin.records.RecordStreamWithMetadata;
import gobblin.stream.RecordEnvelope;
import gobblin.stream.StreamEntity;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import lombok.RequiredArgsConstructor;


/**
 * A {@link Converter} that allows for pipelining asynchronous conversions.
 *
 * The number of outstanding conversions is limited by {@link #MAX_CONCURRENT_ASYNC_CONVERSIONS_KEY} and defaults to
 * {@link #DEFAULT_MAX_CONCURRENT_ASYNC_CONVERSIONS}.
 *
 * Subclasses should implement {@link #convertRecordAsync(Object, Object, WorkUnitState)}.
 */
@Alpha
public abstract class AsyncConverter1to1<SI, SO, DI, DO> extends Converter<SI, SO, DI, DO> {

  public static final String MAX_CONCURRENT_ASYNC_CONVERSIONS_KEY = "gobblin.converter.maxConcurrentAsyncConversions";
  public static final int DEFAULT_MAX_CONCURRENT_ASYNC_CONVERSIONS = 20;

  @Override
  public abstract SO convertSchema(SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException;

  @Override
  public final Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    throw new UnsupportedOperationException("Async converters are only supported in stream mode. Make sure to set "
        + ConfigurationKeys.TASK_SYNCHRONOUS_EXECUTION_MODEL_KEY + " to false.");
  }

  /**
   * Convert the input record asynchronously. Return a {@link CompletableFuture} for the converted record.
   */
  protected abstract CompletableFuture<DO> convertRecordAsync(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException;

  @Override
  public RecordStreamWithMetadata<DO, SO> processStream(RecordStreamWithMetadata<DI, SI> inputStream,
      WorkUnitState workUnitState) throws SchemaConversionException {
    int maxConcurrentAsyncConversions = workUnitState.getPropAsInt(MAX_CONCURRENT_ASYNC_CONVERSIONS_KEY,
        DEFAULT_MAX_CONCURRENT_ASYNC_CONVERSIONS);
    SO outputSchema = convertSchema(inputStream.getSchema(), workUnitState);
    Flowable<StreamEntity<DO>> outputStream =
        inputStream.getRecordStream()
            .flatMapSingle(in -> {
              if (in instanceof ControlMessage) {
                getMessageHandler().handleMessage((ControlMessage) in);
                return Single.just((ControlMessage<DO>) in);
              } else if (in instanceof RecordEnvelope) {
                RecordEnvelope<DI> recordEnvelope = (RecordEnvelope<DI>) in;
                return new SingleAsync(recordEnvelope, convertRecordAsync(outputSchema, recordEnvelope.getRecord(), workUnitState));
              } else {
                throw new IllegalStateException("Expected ControlMessage or RecordEnvelope.");
              }
            }, false, maxConcurrentAsyncConversions);
    return inputStream.withRecordStream(outputStream, outputSchema);
  }

  @RequiredArgsConstructor
  private class SingleAsync extends Single<RecordEnvelope<DO>> {

    private final RecordEnvelope<DI> originalRecord;
    private final CompletableFuture<DO> completableFuture;

    @Override
    protected void subscribeActual(@NonNull SingleObserver<? super RecordEnvelope<DO>> observer) {
      this.completableFuture.thenAccept(d -> observer.onSuccess(originalRecord.withRecord(d))).exceptionally(t -> {
        observer.onError(t);
        return null;
      });
    }
  }
}
