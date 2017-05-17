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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


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
  S getSchema() throws IOException;

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
  default D readRecord(@Deprecated D reuse) throws DataRecordException, IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Modify the output {@link RecordEnvelope} for example to add callbacks or metadata.
   */
  default void modifyEnvelope(RecordEnvelope<D> envelope) {}

  /**
   * Read the next {@link RecordEnvelope}.
   */
  default RecordEnvelope<D> readRecordEnvelope() throws DataRecordException, IOException {
    D record = readRecord(null);
    return record == null ? null : new RecordEnvelope<>(record);
  }

  /**
   * Create the {@link Stream} of {@link RecordEnvelope}s for the pipeline.
   * @param shutdownRequested will become true if a shutdown is requested. The extractor must stop producing records. If
   *                          the extractor does not stop producing records withing the shutdown timeout, the stream will
   *                          be killed with an Exception.
   */
  default Stream<RecordEnvelope<D>> getRecordEnvelopeStream(AtomicBoolean shutdownRequested) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ExtractorIterator<D>(this, shutdownRequested),
        Spliterator.IMMUTABLE | Spliterator.NONNULL), false).peek(this::modifyEnvelope);
  }

  /**
   * Get the expected source record count.
   *
   * @return the expected source record count
   */
  long getExpectedRecordCount();

  /**
   * Get the calculated high watermark up to which data records are to be extracted.
   *
   * @return the calculated high watermark
   * @deprecated there is no longer support for reporting the high watermark via this method, please see
   * <a href="https://github.com/linkedin/gobblin/wiki/Watermarks">Watermarks</a> for more information.
   */
  @Deprecated
  long getHighWatermark();
}
