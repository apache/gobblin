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

package gobblin.source.extractor;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import gobblin.records.RecordStreamWithMetadata;
import gobblin.stream.RecordEnvelope;
import gobblin.stream.StreamEntity;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.BiConsumer;
import javax.annotation.Nullable;


/**
 * An interface for classes that are responsible for extracting data from a data source.
 *
 * <p>
 *     All source specific logic for a data source should be encapsulated in an
 *     implementation of this interface and {@link gobblin.source.Source}.
 * </p>
 *
 * @author kgoodhop
 *
 * @param <S> output schema type
 * @param <D> output record type
 */
public interface Extractor<S, D> extends Closeable {

  /**
   * Get the schema (metadata) of the extracted data records.
   *
   * @return schema of the extracted data records
   * @throws java.io.IOException if there is problem getting the schema
   */
  public S getSchema() throws IOException;

  /**
   * Read the next data record from the data source.
   *
   * <p>
   *   Reuse of data records has been deprecated and is not executed internally.
   * </p>
   *
   * @param reuse the data record object to be reused
   * @return the next data record extracted from the data source
   * @throws DataRecordException if there is problem with the extracted data record
   * @throws java.io.IOException if there is problem extracting the next data record from the source
   */
  @Nullable
  default D readRecord(@Deprecated D reuse) throws DataRecordException, IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the expected source record count.
   *
   * @return the expected source record count
   */
  public long getExpectedRecordCount();

  /**
   * Get the calculated high watermark up to which data records are to be extracted.
   *
   * @return the calculated high watermark
   * @deprecated there is no longer support for reporting the high watermark via this method, please see
   * <a href="https://github.com/linkedin/gobblin/wiki/Watermarks">Watermarks</a> for more information.
   */
  @Deprecated
  public long getHighWatermark();

  /**
   * Read an {@link RecordEnvelope}. By default, just wrap {@link #readRecord(Object)} in a {@link RecordEnvelope}.
   */
  @SuppressWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE",
      justification = "Findbugs believes readRecord(null) is non-null. This is not true.")
  default RecordEnvelope<D> readRecordEnvelope() throws DataRecordException, IOException {
    D record = readRecord(null);
    return record == null ? null : new RecordEnvelope<>(record);
  }

  /**
   * @param shutdownRequest an {@link AtomicBoolean} that becomes true when a shutdown has been requested.
   * @return a {@link Flowable} with the records from this source. Note the flowable should honor downstream backpressure.
   */
  default RecordStreamWithMetadata<D, S> recordStream(AtomicBoolean shutdownRequest) throws IOException {
    S schema = getSchema();
    Flowable<StreamEntity<D>> recordStream = Flowable.generate(() -> shutdownRequest, (BiConsumer<AtomicBoolean, Emitter<StreamEntity<D>>>) (state, emitter) -> {
      if (state.get()) {
        emitter.onComplete();
      }
      try {
        RecordEnvelope<D> record = readRecordEnvelope();
        if (record != null) {
          emitter.onNext(record);
        } else {
          emitter.onComplete();
        }
      } catch (DataRecordException | IOException exc) {
        emitter.onError(exc);
      }
    });
    recordStream = recordStream.doFinally(this::close);
    return new RecordStreamWithMetadata<>(recordStream, schema);
  }

}
