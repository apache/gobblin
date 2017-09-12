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

package org.apache.gobblin.stream;

import java.io.Closeable;
import java.io.IOException;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.records.RecordStreamProcessor;
import org.apache.gobblin.records.RecordStreamWithMetadata;

import io.reactivex.Flowable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * A {@link RecordStreamProcessor} that inspects an input record and outputs control messages before, after, or around
 * the input record
 * @param <SI>
 * @param <DI>
 */
public abstract class ControlMessageInjector<SI, DI> implements Closeable,
        RecordStreamProcessor<SI, SI, DI, DI> {

  @Setter(AccessLevel.PROTECTED)
  @Getter(AccessLevel.PROTECTED)
  private GlobalMetadata<SI> inputGlobalMetadata;

  /**
   * Initialize this {@link ControlMessageInjector}.
   *
   * @param workUnitState a {@link WorkUnitState} object carrying configuration properties
   * @return an initialized {@link ControlMessageInjector} instance
   */
  protected ControlMessageInjector<SI, DI> init(WorkUnitState workUnitState) {
    return this;
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Set the global metadata of the input messages
   * @param inputGlobalMetadata the global metadata for input messages
   * @param workUnitState
   */
  protected void setInputGlobalMetadata(GlobalMetadata<SI> inputGlobalMetadata, WorkUnitState workUnitState) {
    this.inputGlobalMetadata = inputGlobalMetadata;
  }

  /**
   * Inject {@link ControlMessage}s before the record
   * @param inputRecordEnvelope
   * @param workUnitState
   * @return The {@link ControlMessage}s to inject before the record
   */
  protected abstract Iterable<ControlMessage<DI>> injectControlMessagesBefore(RecordEnvelope<DI> inputRecordEnvelope,
      WorkUnitState workUnitState);

  /**
   * Inject {@link ControlMessage}s after the record
   * @param inputRecordEnvelope
   * @param workUnitState
   * @return The {@link ControlMessage}s to inject after the record
   */
  protected abstract Iterable<ControlMessage<DI>> injectControlMessagesAfter(RecordEnvelope<DI> inputRecordEnvelope,
      WorkUnitState workUnitState);

  /**
   * Apply injections to the input {@link RecordStreamWithMetadata}.
   * {@link ControlMessage}s may be injected before, after, or around the input record.
   * A {@link MetadataUpdateControlMessage} will update the current input {@link GlobalMetadata} and pass the
   * updated input {@link GlobalMetadata} to the next processor to propagate the metadata update down the pipeline.
   */
  @Override
  public RecordStreamWithMetadata<DI, SI> processStream(RecordStreamWithMetadata<DI, SI> inputStream,
      WorkUnitState workUnitState) throws StreamProcessingException {
    init(workUnitState);

    setInputGlobalMetadata(inputStream.getGlobalMetadata(), workUnitState);

    Flowable<StreamEntity<DI>> outputStream =
        inputStream.getRecordStream()
            .flatMap(in -> {
              if (in instanceof ControlMessage) {
                ControlMessage out = (ControlMessage) in;
                if (in instanceof MetadataUpdateControlMessage) {
                  setInputGlobalMetadata(((MetadataUpdateControlMessage) in).getGlobalMetadata(),
                      workUnitState);
                  out = new MetadataUpdateControlMessage<SI, DI>(this.inputGlobalMetadata);
                }

                getMessageHandler().handleMessage((ControlMessage) in);
                return Flowable.just(((ControlMessage<DI>) out));

              } else if (in instanceof RecordEnvelope) {
                RecordEnvelope<DI> recordEnvelope = (RecordEnvelope<DI>) in;
                Iterable<ControlMessage<DI>> injectedBeforeIterable =
                    injectControlMessagesBefore(recordEnvelope, workUnitState);
                Iterable<ControlMessage<DI>> injectedAfterIterable =
                    injectControlMessagesAfter(recordEnvelope, workUnitState);

                if (injectedBeforeIterable == null && injectedAfterIterable == null) {
                  // nothing injected so return the record envelope
                  return Flowable.just(recordEnvelope);
                } else {
                  Flowable<StreamEntity<DI>> flowable;

                  if (injectedBeforeIterable != null) {
                    flowable = Flowable.<StreamEntity<DI>>fromIterable(injectedBeforeIterable)
                        .concatWith(Flowable.just(recordEnvelope));
                  } else {
                    flowable = Flowable.just(recordEnvelope);
                  }

                  if (injectedAfterIterable != null) {
                    flowable.concatWith(Flowable.fromIterable(injectedAfterIterable));
                  }
                  return flowable;
                }
              } else {
                throw new UnsupportedOperationException();
              }
            }, 1);
    outputStream = outputStream.doOnComplete(this::close);
    return inputStream.withRecordStream(outputStream, this.inputGlobalMetadata);
  }

  /**
   * @return {@link ControlMessageHandler} to call for each {@link ControlMessage} received.
   */
  protected ControlMessageHandler getMessageHandler() {
    return ControlMessageHandler.NOOP;
  }
}
