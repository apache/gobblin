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

package org.apache.gobblin.source.extractor.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;


/**
 * An implementation of {@link Extractor} that uses a Hadoop {@link RecordReader} to read records
 * from a {@link org.apache.hadoop.mapreduce.lib.input.FileSplit}.
 *
 * <p>
 *   This class can read either keys of type K or values of type V using the
 *   given {@link RecordReader}, depending on the value of the second argument of the constructor
 *   {@link #HadoopFileInputExtractor(RecordReader, boolean)}. It will read keys if the argument
 *   is {@code true}, otherwise it will read values. Normally, this is specified using the property
 *   {@link HadoopFileInputSource#FILE_INPUT_READ_KEYS_KEY}, which is {@code false} by default.
 * </p>
 *
 * <p>
 *   This class provides a default implementation of {@link #readRecord(Object)} that simply casts
 *   the keys or values read by the {@link RecordReader} into type D. It is required
 *   that type K or V can be safely casted to type D.
 * </p>
 *
 * <p>
 *   The Hadoop {@link RecordReader} is passed into this class, which is responsible for closing
 *   it by calling {@link RecordReader#close()} in {@link #close()}.
 * </p>
 *
 * <p>
 *   A concrete implementation of this class should at least implement the {@link #getSchema()}
 *   method.
 * </p>
 *
 * @param S output schema type
 * @param D output data record type that MUST be compatible with either K or V
 * @param K key type expected by the {@link RecordReader}
 * @param V value type expected by the {@link RecordReader}
 *
 * @author Yinan Li
 */
public abstract class HadoopFileInputExtractor<S, D, K, V> implements Extractor<S, D> {

  private final RecordReader<K, V> recordReader;
  private final boolean readKeys;

  public HadoopFileInputExtractor(RecordReader<K, V> recordReader, boolean readKeys) {
    this.recordReader = recordReader;
    this.readKeys = readKeys;
  }

  /**
   * {@inheritDoc}.
   *
   * This method will throw a {@link ClassCastException} if type {@link #<D>} is not compatible
   * with type {@link #<K>} if keys are supposed to be read, or if it is not compatible with type
   * {@link #<V>} if values are supposed to be read.
   */
  @Override
  @SuppressWarnings("unchecked")
  public D readRecord(@Deprecated D reuse) throws DataRecordException, IOException {
    try {
      if (this.recordReader.nextKeyValue()) {
        return this.readKeys ? (D) this.recordReader.getCurrentKey() : (D) this.recordReader.getCurrentValue();
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }

    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    return -1l;
  }

  @Override
  public long getHighWatermark() {
    return -1l;
  }

  @Override
  public void close() throws IOException {
    this.recordReader.close();
  }
}
